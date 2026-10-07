"""Leakage and candidate contracts for the new diagnostic protocols."""
import unittest
from pathlib import Path
import pandas as pd
from benchmark.model_quality.prepare_movielens import split_ratings

class AlternativeContracts(unittest.TestCase):
    def test_pair_split_is_order_independent_and_disjoint(self):
        f=pd.DataFrame({'user_id':['u']*100,'video_id':list(map(str,range(100))),'time_ms':range(100)})
        a=split_ratings(f,'pair').set_index('video_id').split
        b=split_ratings(f.sample(frac=1,random_state=7),'pair').set_index('video_id').split
        self.assertTrue(a.sort_index().equals(b.sort_index()));self.assertEqual(set(a),{'train','valid','test'})

    def test_chronological_split_keeps_boundary_ties(self):
        f=pd.DataFrame({'user_id':['u']*20,'video_id':list(map(str,range(20))),'time_ms':list(range(15))+[15]*3+[18,19]})
        a=split_ratings(f,'user-time')
        self.assertEqual(a[a.time_ms.eq(15)].split.nunique(),1)
        self.assertLess(a[a.split.eq('train')].time_ms.max(),a[a.split.eq('valid')].time_ms.min())
        self.assertLess(a[a.split.eq('valid')].time_ms.max(),a[a.split.eq('test')].time_ms.min())
