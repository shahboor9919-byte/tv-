from utils.helpers import extract_resolution


class Scorer:
    def __init__(self, config):
        scoring = config.get('scoring', {})
        validation = config.get('validation', {})

        self.speed_weight = scoring.get('speed_weight', 0.3)
        self.resolution_weight = scoring.get('resolution_weight', 0.3)
        self.stability_weight = scoring.get('stability_weight', 0.2)
        self.format_weight = scoring.get('format_weight', 0.2)

        self.res_boost = scoring.get('resolution_boost', {
            2160: 100,
            1080: 80,
            720: 60,
            480: 40,
            0: 10
        })

        self.format_boost = scoring.get('format_boost', {
            'm3u8': 100,
            'ts': 70,
            'other': 40
        })

        self.penalty_slow = scoring.get('penalty_slow', 10)
        self.penalty_unstable = scoring.get('penalty_unstable', 20)
        self.penalty_redirect = 5

        self.max_latency = validation.get('max_latency', 3.0)

    def score_stream(self, stream):
        if not stream.get('valid', False):
            return 0.0

        latency = stream.get('latency', self.max_latency)

        if latency <= 0:
            speed_score = 100
        else:
            speed_score = max(0, 100 * (1 - (latency / self.max_latency)))

        text = f"{stream.get('name', '')} {stream.get('group', '')} {stream.get('tvg_id', '')}"
        res = extract_resolution(text)
        res_score = self.res_boost.get(res, 0)

        status = stream.get('status')

        if status == 200:
            stability_score = 100
        elif isinstance(status, int) and 200 <= status < 300:
            stability_score = 80
        else:
            stability_score = 0

        url = stream.get('url', '').lower()

        if '.m3u8' in url:
            format_score = self.format_boost.get('m3u8', 100)
        elif '.ts' in url:
            format_score = self.format_boost.get('ts', 70)
        else:
            format_score = self.format_boost.get('other', 40)

        score = (
            speed_score * self.speed_weight +
            res_score * self.resolution_weight +
            stability_score * self.stability_weight +
            format_score * self.format_weight
        )

        if latency > self.max_latency:
            score -= self.penalty_slow

        if isinstance(status, int) and status >= 300:
            score -= self.penalty_redirect

        if not stream.get('valid', False):
            score -= self.penalty_unstable

        return max(0, min(100, score))

    def score_all(self, streams):
        for s in streams:
            s['score'] = self.score_stream(s)
        return streams
