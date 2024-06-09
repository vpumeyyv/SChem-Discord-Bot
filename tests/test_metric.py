#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
import unittest
import sys

import schem

# Insert the parent directory so the test package is accessible even if it's not available system-wide
sys.path.insert(1, str(Path(__file__).parent.parent))

import metric

test_soln_str = """SOLUTION:Accidents Happen,Zig,275-1-88,Cycles
COMPONENT:'drag-advanced-reactor',2,0,''
MEMBER:'instr-start',0,0,128,4,1,0,0
MEMBER:'instr-start',90,0,32,1,0,0,0
MEMBER:'feature-bonder',-1,0,1,1,4,0,0
MEMBER:'feature-bonder',-1,0,1,2,5,0,0
MEMBER:'feature-bonder',-1,0,1,1,5,0,0
MEMBER:'feature-bonder',-1,0,1,2,6,0,0
MEMBER:'feature-sensor',-1,0,1,1,1,0,0
MEMBER:'instr-arrow',180,0,64,1,4,0,0
MEMBER:'instr-arrow',90,0,64,0,4,0,0
MEMBER:'instr-arrow',0,0,64,0,5,0,0
MEMBER:'instr-grab',-1,1,128,0,5,0,0
MEMBER:'instr-grab',-1,1,128,1,5,0,0
MEMBER:'instr-grab',-1,2,128,1,4,0,0
MEMBER:'instr-input',-1,0,128,1,2,0,0
MEMBER:'instr-arrow',90,0,64,1,1,0,0
MEMBER:'instr-grab',-1,2,128,7,4,0,0
MEMBER:'instr-grab',-1,2,128,7,3,0,0
MEMBER:'instr-bond',-1,1,128,7,2,0,0
MEMBER:'instr-bond',-1,0,128,6,2,0,0
MEMBER:'instr-bond',-1,0,128,3,2,0,0
MEMBER:'instr-bond',-1,0,128,4,3,0,0
MEMBER:'instr-rotate',-1,0,128,2,4,0,0
MEMBER:'instr-grab',-1,2,128,2,3,0,0
MEMBER:'instr-grab',-1,1,128,3,4,0,0
MEMBER:'instr-sensor',90,0,128,4,2,0,14
MEMBER:'instr-arrow',180,0,64,7,2,0,0
MEMBER:'instr-arrow',-90,0,64,7,4,0,0
MEMBER:'instr-arrow',180,0,64,2,1,0,0
MEMBER:'instr-grab',-1,1,128,1,1,0,0
MEMBER:'instr-arrow',90,0,64,1,5,0,0
MEMBER:'instr-arrow',0,0,64,1,7,0,0
MEMBER:'instr-arrow',-90,0,64,2,7,0,0
MEMBER:'instr-sensor',0,0,128,2,5,0,14
MEMBER:'instr-arrow',0,0,64,3,4,0,0
MEMBER:'instr-arrow',-90,0,64,5,5,0,0
MEMBER:'instr-arrow',-90,0,64,7,3,0,0
MEMBER:'instr-sync',-1,0,128,4,4,0,0
MEMBER:'instr-rotate',-1,1,128,6,4,0,0
MEMBER:'instr-bond',-1,0,128,2,7,0,0
MEMBER:'instr-bond',-1,0,128,1,6,0,0
MEMBER:'instr-input',-1,1,128,1,7,0,0
MEMBER:'instr-input',-1,0,128,5,1,0,0
MEMBER:'instr-grab',-1,1,32,1,1,0,0
MEMBER:'instr-arrow',0,0,16,1,6,0,0
MEMBER:'instr-arrow',-90,0,16,2,6,0,0
MEMBER:'instr-sensor',0,0,32,2,5,0,15
MEMBER:'instr-sensor',180,0,32,4,5,0,14
MEMBER:'instr-bond',-1,1,32,3,5,0,0
MEMBER:'instr-arrow',180,0,16,2,1,0,0
MEMBER:'instr-arrow',90,0,16,1,1,0,0
MEMBER:'instr-bond',-1,0,32,2,6,0,0
MEMBER:'instr-input',-1,1,128,5,4,0,0
MEMBER:'instr-input',-1,0,32,1,2,0,0
MEMBER:'instr-arrow',0,0,16,0,1,0,0
MEMBER:'instr-arrow',-90,0,16,0,5,0,0
MEMBER:'instr-grab',-1,1,32,1,5,0,0
MEMBER:'instr-grab',-1,2,32,0,5,0,0
MEMBER:'instr-bond',-1,0,32,1,4,0,0
MEMBER:'instr-input',-1,1,128,5,2,0,0
MEMBER:'instr-bond',-1,1,32,1,6,0,0
MEMBER:'instr-grab',-1,2,32,2,4,0,0
MEMBER:'instr-sync',-1,0,32,2,3,0,0
MEMBER:'instr-bond',-1,1,32,4,6,0,0
MEMBER:'instr-grab',-1,2,32,7,5,0,0
MEMBER:'instr-arrow',-90,0,16,7,5,0,0
MEMBER:'instr-toggle',0,0,32,7,4,0,0
MEMBER:'instr-arrow',180,0,16,7,4,0,0
MEMBER:'instr-arrow',-90,0,16,9,4,0,0
MEMBER:'instr-arrow',180,0,16,9,2,0,0
MEMBER:'instr-arrow',90,0,16,4,4,0,0
MEMBER:'instr-input',-1,1,32,6,4,0,0
MEMBER:'instr-arrow',180,0,64,4,3,0,0
MEMBER:'instr-arrow',90,0,64,3,3,0,0
MEMBER:'instr-bond',-1,0,32,4,4,0,0
MEMBER:'instr-arrow',-90,0,16,3,7,0,0
MEMBER:'instr-arrow',0,0,16,3,3,0,0
MEMBER:'instr-grab',-1,1,32,4,3,0,0
MEMBER:'instr-arrow',180,0,16,4,7,0,0
MEMBER:'instr-rotate',-1,0,32,5,3,0,0
MEMBER:'instr-arrow',90,0,16,7,3,0,0
MEMBER:'instr-grab',-1,2,32,7,3,0,0
MEMBER:'instr-bond',-1,0,32,9,3,0,0
MEMBER:'instr-sensor',90,0,32,3,2,0,15
MEMBER:'instr-bond',-1,1,32,8,2,0,0
MEMBER:'instr-bond',-1,0,32,6,2,0,0
MEMBER:'instr-output',-1,0,32,1,3,0,0
MEMBER:'instr-arrow',0,0,64,5,3,0,0
MEMBER:'instr-input',-1,1,128,6,1,0,0
MEMBER:'instr-arrow',90,0,64,7,1,0,0
MEMBER:'instr-arrow',-90,0,64,2,2,0,0
MEMBER:'instr-bond',-1,1,32,5,4,0,0
MEMBER:'instr-output',-1,0,128,0,4,0,0
MEMBER:'instr-arrow',-90,0,16,2,2,0,0
MEMBER:'instr-output',-1,1,32,2,2,0,0
MEMBER:'instr-output',-1,1,128,2,2,0,0
PIPE:0,4,1
PIPE:1,4,2'''"""

soln = schem.Solution(test_soln_str)


class TestGame(unittest.TestCase):
    def test_eval_metric_base_metrics(self):
        """Test the reactor."""
        self.assertEqual(metric.eval_metric(soln, "cycles"), 275, "Incorrect cycles metric")
        self.assertEqual(metric.eval_metric(soln, "reactors"), 1, "Incorrect reactors metric")
        self.assertEqual(metric.eval_metric(soln, "symbols"), 88, "Incorrect symbols metric")
        self.assertEqual(metric.eval_metric(soln, "waldos"), 2, "Incorrect waldos metric")
        self.assertEqual(metric.eval_metric(soln, "waldopath"), 53, "Incorrect waldopath metric")
        self.assertEqual(metric.eval_metric(soln, "bonders"), 4, "Incorrect bonders metric")
        self.assertEqual(metric.eval_metric(soln, "arrows"), 34, "Incorrect arrows metric")
        self.assertEqual(metric.eval_metric(soln, "input_instrs"), 8, "Incorrect input_instrs metric")
        self.assertEqual(metric.eval_metric(soln, "alpha_input_instrs"), 3, "Incorrect alpha_input_instrs metric")
        self.assertEqual(metric.eval_metric(soln, "beta_input_instrs"), 5, "Incorrect beta_input_instrs metric")
        self.assertEqual(metric.eval_metric(soln, "output_instrs"), 4, "Incorrect output_instrs metric")
        self.assertEqual(metric.eval_metric(soln, "psi_output_instrs"), 2, "Incorrect psi_output_instrs metric")
        self.assertEqual(metric.eval_metric(soln, "omega_output_instrs"), 2, "Incorrect omega_output_instrs metric")
        self.assertEqual(metric.eval_metric(soln, "flip_flops"), 1, "Incorrect flip_flops metric")
        self.assertEqual(metric.eval_metric(soln, "senses"),  5, "Incorrect senses metric")
        self.assertEqual(metric.eval_metric(soln, "rotates"), 3, "Incorrect rotates metric")
        self.assertEqual(metric.eval_metric(soln, "syncs"), 2, "Incorrect syncs metric")
        self.assertEqual(metric.eval_metric(soln, "bond_pluses"), 10, "Incorrect bond_pluses metric")
        self.assertEqual(metric.eval_metric(soln, "bond_minuses"), 6, "Incorrect bond_minuses metric")
        self.assertEqual(metric.eval_metric(soln, "bonds"), 16, "Incorrect bonds metric")
        self.assertEqual(metric.eval_metric(soln, "fuses"), 0, "Incorrect fuses metric")
        self.assertEqual(metric.eval_metric(soln, "splits"), 0, "Incorrect splits metric")
        self.assertEqual(metric.eval_metric(soln, "swaps"), 0, "Incorrect swaps metric")
        self.assertEqual(metric.eval_metric(soln, "recycler_pipes"), 0, "Incorrect recycler_pipes metric")
        self.assertEqual(metric.eval_metric(soln, "max_symbols"), 88, "Incorrect max_symbols metric")
        self.assertEqual(metric.eval_metric(soln, "symbol_footprint"), 47, "Incorrect symbol_footprint metric")
        self.assertEqual(metric.eval_metric(soln, "max_symbol_footprint"), 47, "Incorrect max_symbol_footprint metric")


if __name__ == '__main__':
    unittest.main(verbosity=0, exit=False)
