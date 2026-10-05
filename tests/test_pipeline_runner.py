"""Check runner control flow without connecting to a database."""

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run_pipeline


class PipelineRunnerTests(unittest.TestCase):
    def test_stops_after_any_failed_step(self):
        steps = [(f"step {n}", ["command", str(n)]) for n in range(4)]
        for failed_index in range(len(steps)):
            with self.subTest(failed_index=failed_index):
                effects = [None] * failed_index + [
                    subprocess.CalledProcessError(7, steps[failed_index][1])
                ]
                output = StringIO()
                with patch.object(run_pipeline.subprocess, "run", side_effect=effects) as run:
                    with redirect_stdout(output), redirect_stderr(output):
                        result = run_pipeline.execute_steps(steps)
                self.assertEqual(result, 1)
                self.assertEqual(run.call_count, failed_index + 1)
                self.assertNotIn("Pipeline completed successfully", output.getvalue())

    def test_success_runs_all_steps_in_order(self):
        steps = [("first", ["first-command"]), ("second", ["second-command"])]
        output = StringIO()
        with patch.object(run_pipeline.subprocess, "run") as run:
            with redirect_stdout(output):
                result = run_pipeline.execute_steps(steps)
        self.assertEqual(result, 0)
        self.assertEqual([call.args[0] for call in run.call_args_list],
                         [command for _, command in steps])
        self.assertTrue(all(call.kwargs["check"] for call in run.call_args_list))
        self.assertIn("Pipeline completed successfully", output.getvalue())


if __name__ == "__main__":
    unittest.main(verbosity=2)
