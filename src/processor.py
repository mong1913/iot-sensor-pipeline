from datetime import datetime, timedelta

class SensorProcessor:
    def __init__(self):
        self.window_data = {}

    def add_and_get_average(self, data):
        sensor_id = data['sensor_id']
        val = float(data['value'])
        now = datetime.fromisoformat(data['timestamp'])

        if sensor_id not in self.window_data:
            self.window_data[sensor_id] = []
        self.window_data[sensor_id].append((now, val))

        ten_min_ago = now - timedelta(minutes=10)
        self.window_data[sensor_id] = [d for d in self.window_data[sensor_id] if d[0] > ten_min_ago]

        return sum(d[1] for d in self.window_data[sensor_id]) / len(self.window_data[sensor_id])