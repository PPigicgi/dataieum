import os
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import subprocess

from cleanup import selected_files, fully_reclaimable, removable, make_plan


class CleanupSafety(unittest.TestCase):
    def test_registry_cleanup_protects_active_images_and_unrelated_repositories(self):
        with tempfile.TemporaryDirectory() as tmp:
            active=Path(tmp)/'active.json';active.write_text(json.dumps({'services':{'atlas':{'image':'active'}}}))
            def inspect(*args,**kwargs):return subprocess.CompletedProcess(args,0,json.dumps([{'Id':args[-1]}]))
            rows=[{'ID':ident,'Repository':repo,'Tag':'old'} for ident,repo in [('active','ghcr.io/manzigit/wanted-atlas'),('stale','ghcr.io/manzigit/wanted-luna'),('unrelated','ghcr.io/another/wanted-atlas')]]
            with patch('cleanup.containers',return_value=[]),patch('cleanup.command',side_effect=inspect),patch('cleanup.image_rows',return_value=rows),patch('cleanup.check_file_readers'):
                self.assertEqual([r['id'] for r in make_plan(active,active,[])['images']],['stale'])

    def test_latest_maintenance_undo_images_remain_protected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);active=root/'active.json';marker=root/'current.json'
            active.write_text(json.dumps({'services':{'atlas':{'image':'active'}}}))
            marker.write_text(json.dumps({'root':tmp}))
            for name,image in [('compose.before.json','before'),('compose.updated.json','updated')]:
                (root/name).write_text(json.dumps({'services':{'vector':{'image':image}}}))
            def mapped(path):
                return marker if str(path)=='/opt/wanted/refresh/maintenance-current.json' else Path(path)
            def inspect(*args,**kwargs):
                return subprocess.CompletedProcess(args,0,json.dumps([{'Id':args[-1]}]))
            with patch('cleanup.Path',side_effect=mapped),patch('cleanup.containers',return_value=[]),patch('cleanup.command',side_effect=inspect),patch('cleanup.image_rows',return_value=[]),patch('cleanup.check_file_readers'):
                self.assertEqual(make_plan(active,active,[])['protected_images'],['active','before','updated'])

    def test_live_mount_and_parent_are_protected(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'catalog.sqlite3'; p.write_bytes(b'x')
            with self.assertRaises(ValueError): selected_files([str(p)], [tmp])
            with self.assertRaises(ValueError): selected_files([str(p)], [str(p)])

    def test_shared_hardlink_is_not_reclaimed_until_all_links_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = Path(tmp) / 'a'; a.write_bytes(b'x' * 8192)
            b = Path(tmp) / 'b'; os.link(a, b)
            self.assertEqual(fully_reclaimable(selected_files([a], [])), 0)
            self.assertEqual(fully_reclaimable(selected_files([a, b], [])), a.stat().st_blocks * 512)

    def test_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = Path(tmp) / 'a'; a.write_bytes(b'x')
            b = Path(tmp) / 'b'; b.symlink_to(a)
            with self.assertRaises(ValueError): selected_files([b], [])

    def test_service_and_running_job_are_not_cleanup_candidates(self):
        for name, state in [('dataieum-ontology-20260919-atlas-1', 'exited'),
                            ('dataieum-keyword-build', 'running'), ('other-job', 'exited')]:
            self.assertFalse(removable({'Name': name, 'State': {'Status': state}}))
        self.assertTrue(removable({'Name': 'dataieum-keyword-build', 'State': {'Status': 'exited'}}))

    def test_missing_running_image_fails_during_plan_before_any_deletion(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / 'compose.json'; config.write_text('{"services":{}}')
            live = {'Name': 'service', 'Image': 'sha256:missing', 'State': {'Status': 'running'}}
            with patch('cleanup.containers', return_value=[live]), patch('cleanup.command') as cmd:
                cmd.side_effect = subprocess.CalledProcessError(1, 'image inspect')
                with self.assertRaises(subprocess.CalledProcessError): make_plan(config, config, [])
                cmd.assert_called_once_with('docker', 'image', 'inspect', 'sha256:missing')


if __name__ == '__main__': unittest.main()
