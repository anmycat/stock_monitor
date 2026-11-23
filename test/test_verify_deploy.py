"""D79 分级部署校验专项测试。"""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "deploy"))
import verify_deploy as vd


class TestClassify(unittest.TestCase):
    def test_tiers(self):
        cases = {
            "quant_engine/config.py": "A",
            "UX/static/app.js": "A",
            "rust_accel/src/lib.rs": "A",
            "deploy/update_ecs.sh": "A",
            "deploy/build_rust_accel.sh": "A",
            "rust_accel/Cargo.toml": "B",
            "quant_engine/.env.example": "B",  # 模板文档必须校验（D77 教训：曾被误排除致静默不同步）
            "quant_engine/requirements.txt": "B",
            "README.md": "B",
            "quant_engine/utils/_qerust/qerust.so": "C",
            "quant_engine/utils/_qerust/qerust.dylib": "C",
        }
        for rel, expected in cases.items():
            self.assertEqual(vd.classify(Path(rel)), expected, rel)

    def test_excluded(self):
        for rel in (
            "quant_engine/__pycache__/x.py",
            "rust_accel/target/release/libqerust.so",
            "rust_accel/targetRajAQB/CACHEDIR.TAG",  # cargo 随机后缀 target 目录
            ".env",
            "quant_engine/.env.bak_d56",
            "logs/worker.log",
            "history.db-wal",
            ".DS_Store",
        ):
            self.assertIsNone(vd.classify(Path(rel)), rel)


class TestBuildManifest(unittest.TestCase):
    def test_collects_tiers_and_skips_state(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "quant_engine").mkdir()
            (root / "rust_accel" / "src").mkdir(parents=True)
            (root / "quant_engine" / "config.py").write_text("X=1\n")
            (root / "quant_engine" / ".env").write_text("SECRET=xxx\n")
            (root / "quant_engine" / ".env.example").write_text("SECRET=\n")
            (root / "quant_engine" / "app.log").write_text("log\n")
            (root / "rust_accel" / "Cargo.toml").write_text("[package]\n")
            (root / "rust_accel" / "src" / "lib.rs").write_text("fn main() {}\n")
            (root / "rust_accel" / "target").mkdir()
            (root / "rust_accel" / "target" / "libqerust.so").write_bytes(b"elf")
            m = vd.build_manifest(root)
            self.assertEqual(set(m["tiers"]["A"]), {"quant_engine/config.py", "rust_accel/src/lib.rs"})
            self.assertEqual(set(m["tiers"]["B"]),
                             {"rust_accel/Cargo.toml", "quant_engine/.env.example"})
            self.assertEqual(m["tiers"]["C"], {})  # target/ 被排除
            self.assertGreaterEqual(m["skipped"], 3)  # .env / log / target.so


class TestCompare(unittest.TestCase):
    @staticmethod
    def _m(a=None, b=None, c=None):
        return {"tiers": {"A": a or {}, "B": b or {}, "C": c or {}}, "skipped": 0}

    def test_identical_passes(self):
        m = self._m({"a.py": "h1"}, {"c.toml": "h2"}, {"x.so": "present"})
        ok, lines = vd.compare(m, m)
        self.assertTrue(ok)
        self.assertIn("OK", " ".join(lines))

    def test_code_hash_change_fails(self):
        local = self._m({"a.py": "h1"})
        remote = self._m({"a.py": "h2"})
        ok, lines = vd.compare(local, remote)
        self.assertFalse(ok)
        self.assertIn("内容不同 1", " ".join(lines))

    def test_missing_file_fails(self):
        local = self._m({"a.py": "h1", "b.py": "h2"})
        remote = self._m({"a.py": "h1"})
        ok, lines = vd.compare(local, remote)
        self.assertFalse(ok)
        self.assertIn("仅本地 1", " ".join(lines))

    def test_artifact_bytes_ignored_but_presence_required(self):
        local = self._m(c={"qerust.so": "present"})
        remote = self._m(c={"qerust.so": "present"})
        ok, _ = vd.compare(local, remote)  # 不比 hash（清单里 C 恒为 present）
        self.assertTrue(ok)
        remote_missing = self._m(c={})
        ok, lines = vd.compare(local, remote_missing)
        self.assertFalse(ok)
        self.assertIn("远端缺失 1", " ".join(lines))

    def test_state_files_never_compared(self):
        local = self._m(a={"x.py": "h"}, b={"c.toml": "t"})
        remote = self._m(a={"x.py": "h"}, b={"c.toml": "t"})
        ok, _ = vd.compare(local, remote)
        self.assertTrue(ok)


class TestCLI(unittest.TestCase):
    def test_manifest_and_compare_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "quant_engine").mkdir()
            (root / "quant_engine" / "config.py").write_text("X=1\n")
            out = Path(td) / "m.json"
            import subprocess
            r = subprocess.run([sys.executable, str(Path(vd.__file__)), "manifest", str(root)],
                               capture_output=True, text=True, check=False)
            self.assertEqual(r.returncode, 0, r.stderr)
            out.write_text(r.stdout)
            r2 = subprocess.run([sys.executable, str(Path(vd.__file__)), "compare", str(out), str(out)],
                                capture_output=True, text=True, check=False)
            self.assertEqual(r2.returncode, 0, r2.stdout + r2.stderr)
            self.assertIn("通过", r2.stdout)


if __name__ == "__main__":
    unittest.main()
