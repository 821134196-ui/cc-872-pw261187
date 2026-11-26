"""测试共用：可精确推进的假时钟（秒级 monotonic 接口，broker 内部转毫秒）。"""


class FakeClock:
    def __init__(self):
        self._t = 0.0

    def __call__(self) -> float:
        return self._t

    def advance(self, ms: int) -> None:
        self._t += ms / 1000.0
