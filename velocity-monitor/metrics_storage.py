from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

class MetricsWindow:
    def __init__(self, window_size_seconds: int = 86400):
        self.window_size = window_size_seconds
        self.lag_history = defaultdict(deque)
        self.last_state: Dict[str, Dict] = {}

    def record(self, group_id: str, lag: int, consumed_offset: int, log_end_offset: int, timestamp: Optional[datetime] = None):
        if timestamp is None:
            timestamp = datetime.utcnow()
        self.lag_history[group_id].append((timestamp, lag))
        self._cleanup_old_entries(group_id, timestamp)

    def _cleanup_old_entries(self, group_id: str, current_time: datetime):
        cutoff = current_time - timedelta(seconds=self.window_size)
        while self.lag_history[group_id] and self.lag_history[group_id][0][0] < cutoff:
            self.lag_history[group_id].popleft()

    def get_lag_average_24h(self, group_id: str) -> float:
        if group_id not in self.lag_history or not self.lag_history[group_id]:
            return 0.0
        lags = [lag for _, lag in self.lag_history[group_id]]
        return sum(lags) / len(lags) if lags else 0.0

    def compute_speeds(self, group_id: str, current_consumed_offset: int, current_log_end_offset: int, current_time: Optional[datetime] = None) -> Tuple[float, float]:
        if current_time is None:
            current_time = datetime.utcnow()

        last_state = self.last_state.get(group_id)
        if not last_state:
            self.last_state[group_id] = {"timestamp": current_time, "consumed_offset": current_consumed_offset, "log_end_offset": current_log_end_offset}
            return 0.0, 0.0

        time_delta = (current_time - last_state["timestamp"]).total_seconds()
        if time_delta == 0:
            return 0.0, 0.0

        consumed_delta = current_consumed_offset - last_state["consumed_offset"]
        log_end_delta = current_log_end_offset - last_state["log_end_offset"]

        read_speed = max(0, consumed_delta / time_delta)
        write_speed = max(0, log_end_delta / time_delta)

        self.last_state[group_id] = {"timestamp": current_time, "consumed_offset": current_consumed_offset, "log_end_offset": current_log_end_offset}
        return read_speed, write_speed

    def get_lag_percentile_95(self, group_id: str) -> float:
        if group_id not in self.lag_history or not self.lag_history[group_id]:
            return 0.0
        lags = sorted([lag for _, lag in self.lag_history[group_id]])
        if not lags:
            return 0.0
        index = int(len(lags) * 0.95)
        return lags[min(index, len(lags) - 1)]
