import sys
import tempfile
import unittest
from dataclasses import asdict
from datetime import date, datetime, time, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ofi_vpin_strategy import (  # noqa: E402
    BRT, Config, Event, OnlineOFI, OnlineVPIN, PaperBroker, Strategy, run_events, run_replay,
)
from profitdll_feed import ProfitDLLFeed, Recorder, EventBuilder, run_live  # noqa: E402
from synthetic import SimulatedProfitDLL  # noqa: E402


def ev(ts, session="2026-08-06", bid=176900.0, ask=176905.0, bid_size=100.0, ask_size=100.0,
       last=None, volume=0.0, side=0):
    return Event(ts, session, bid, ask, bid_size, ask_size, bid if last is None else last, volume, side)


def at(day, hh, mm, ss=0):
    return datetime(2026, 8, day, hh, mm, ss, tzinfo=BRT)


class OFITest(unittest.TestCase):
    def test_cont_formula(self):
        ofi = OnlineOFI()
        t = at(6, 10, 0)
        self.assertEqual(ofi.update(ev(t, bid_size=100, ask_size=100)), 0.0)
        # Bid sobe: +30 (tamanho novo). Ask sobe: oferta anterior (100) saiu, conta +100.
        self.assertEqual(ofi.update(ev(t, bid=176905, ask=176910, bid_size=30, ask_size=80)), 30 + 100)
        # Mesmos preços: bid sem variação; ask perde 30 contratos, conta +30.
        self.assertEqual(ofi.update(ev(t, bid=176905, ask=176910, bid_size=30, ask_size=50)), 30)
        # Ask cai para nível novo: -tamanho novo do ask.
        self.assertEqual(ofi.update(ev(t, bid=176905, ask=176907.5, bid_size=30, ask_size=40)), -40)


class VPINTest(unittest.TestCase):
    def test_buckets_and_percentile(self):
        vpin = OnlineVPIN(volume_bucket=10, L=2, rank_window=50)
        self.assertIsNone(vpin.update("s", 10, 1))
        self.assertEqual(vpin.update("s", 10, -1), 1.0)
        for _ in range(40):
            vpin.update("s", 5, 1)
            vpin.update("s", 5, -1)
        self.assertEqual(vpin.last_vpin, 0.0)
        self.assertEqual(vpin.last_rank, 0.0)
        vpin.update("s", 10, 1)
        self.assertGreater(vpin.last_rank, 0.9)

    def test_resets_on_new_session(self):
        vpin = OnlineVPIN(volume_bucket=10, L=1)
        vpin.update("a", 10, 1)
        self.assertEqual(vpin.last_vpin, 1.0)
        vpin.update("b", 0, 0)
        self.assertIsNone(vpin.last_vpin)


class BrokerTest(unittest.TestCase):
    def setUp(self):
        self.cfg = Config(slippage_ticks=0, fee_brl_per_contract=0.0)
        self.broker = PaperBroker(self.cfg)

    def test_stop_gap_fills_at_traded_price(self):
        self.broker.open(1, 1000.0, 1, 10, "t", at(6, 10, 0), "s")
        self.broker.mark_and_manage(ev(at(6, 10, 1), last=900.0))
        self.assertEqual(self.broker.trades[0].exit_price, 900.0)
        self.assertEqual(self.broker.trades[0].exit_reason, "stop")

    def test_target_needs_trade_through(self):
        self.broker.open(-1, 1000.0, 1, 10, "t", at(6, 10, 0), "s")
        target = self.broker.position.target_price
        self.broker.mark_and_manage(ev(at(6, 10, 1), last=target))
        self.assertIsNotNone(self.broker.position)
        self.broker.mark_and_manage(ev(at(6, 10, 2), last=target - self.cfg.tick_size))
        self.assertEqual(self.broker.trades[0].exit_price, target)


class StrategySessionTest(unittest.TestCase):
    def test_no_overnight_position_and_finish_never_opens(self):
        s = Strategy(Config())
        s.on_event(ev(at(6, 10, 0)))
        s.broker.open(1, 176905.0, 1, 82, "t", at(6, 10, 0), "2026-08-06")
        s.on_event(ev(at(6, 10, 1), last=176900.0))
        s.on_event(ev(at(7, 9, 30), session="2026-08-07", last=177500.0))
        self.assertEqual([t.exit_reason for t in s.broker.trades], ["session_end"])
        self.assertTrue(s.broker.trades[0].exit_time.startswith("2026-08-06"))
        s.finish()
        self.assertIsNone(s.broker.position)

    def test_daily_loss_limit_is_per_session(self):
        cfg = Config(daily_loss_limit_brl=100.0)
        s = Strategy(cfg)
        s.on_event(ev(at(6, 10, 0)))
        s.broker.realized_pnl = -150.0
        self.assertTrue(s.broker.realized_pnl - s.session_start_pnl <= -cfg.daily_loss_limit_brl)
        s.on_event(ev(at(7, 10, 0), session="2026-08-07"))
        self.assertEqual(s.session_start_pnl, -150.0)
        self.assertEqual(s.broker.realized_pnl - s.session_start_pnl, 0.0)

    def test_feed_gap_discards_window(self):
        s = Strategy(Config(max_feed_staleness_ms=1500))
        s.on_event(ev(at(6, 10, 0), volume=5, side=1))
        s.on_event(ev(at(6, 10, 0) + timedelta(seconds=5), volume=5, side=1))
        self.assertEqual(s.window_start, at(6, 10, 0) + timedelta(seconds=5))
        self.assertEqual(sum(s.funnel.values()), 0)

    def test_infers_unknown_aggressor_by_quote_rule(self):
        s = Strategy(Config())
        self.assertEqual(s._aggressor(ev(at(6, 10, 0), last=176905.0, volume=1, side=0)), 1)
        self.assertEqual(s._aggressor(ev(at(6, 10, 0), last=176900.0, volume=1, side=0)), -1)


class FeedTest(unittest.TestCase):
    def test_event_builder_waits_for_both_sides_and_skips_edits(self):
        b = EventBuilder()
        t = at(6, 10, 0)
        self.assertIsNone(b.on_book(0, 176900.0, 10, t))
        raw = b.on_book(1, 176905.0, 12, t)
        self.assertEqual((raw["bid"], raw["ask"], raw["last_volume"]), (176900.0, 176905.0, 0.0))
        trade = b.on_trade(176905.0, 3, 2, "06/08/2026 10:00:00.123", "\x00", t)
        self.assertEqual((trade["aggressor_side"], trade["last_volume"]), (1, 3.0))
        self.assertEqual(trade["exchange_time"], "2026-08-06T10:00:00.123-03:00")
        self.assertIsNone(b.on_trade(176905.0, 3, 2, "06/08/2026 10:00:00.200", "\x01", t))
        self.assertEqual(b.edits_skipped, 1)

    def test_virtual_run_matches_replay_of_recording(self):
        cfg = Config(vpin_threshold_mode="percentile", min_vpin=0.7, max_vpin=1.0)
        with tempfile.TemporaryDirectory() as tmp:
            dll = SimulatedProfitDLL(day=date(2026, 8, 6), end=time(17, 5))
            feed = ProfitDLLFeed(dll, "WINZ26", "F", clock=dll.clock_ns)
            feed.initialize("k", "u", "p")
            feed.wait_connected(5)
            feed.subscribe()
            live = Strategy(cfg)
            recorder = Recorder(Path(tmp), "WINZ26")
            run_live(feed, live, recorder, time(17, 5), warn_lag_ms=None)
            recorder.close()
            feed.close()
            live.finish()
            replay = run_replay([Path(tmp)], cfg)
        self.assertGreater(len(live.broker.trades), 0)
        self.assertEqual(replay["trades"], [asdict(t) for t in live.broker.trades])


class SelfTest(unittest.TestCase):
    def test_self_test_runs(self):
        cfg = Config(volume_bucket=2, vpin_L=2)
        events = [ev(at(6, 9, 0) + timedelta(milliseconds=100 * i), volume=1, side=-1) for i in range(20)]
        strategy, count = run_events(events, cfg)
        self.assertEqual(count, 20)
        self.assertIsNone(strategy.broker.position)


if __name__ == "__main__":
    unittest.main()
