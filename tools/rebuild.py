#!/usr/bin/env python3
"""CNN-DATA-BOT: сборка полных кадров из ключевого кадра и изменений.

Как модуль:
    from rebuild import Recording
    rec = Recording("data/Steve_2026-11-10T12-00-00-000Z")
    for tick, origin, blocks, entities in rec.frames(step=20):
        ...   # blocks: np.uint16 [y][z][x], size^3

Из командной строки:
    python3 tools/rebuild.py data/Steve_...            # информация о записи
    python3 tools/rebuild.py data/Steve_... --tick 600 --out frame.npz
"""
import argparse
import gzip
import json
import os
import struct

import numpy as np

UNKNOWN = 65535
R_SECTION, R_BLOCK, R_ORIGIN, R_DROP = 1, 2, 3, 4
HEAD = struct.Struct("<BIiii")  # тип, тик, a, b, c (17 байт)


class Recording:
    def __init__(self, base):
        if base.endswith(".json"):
            base = base[:-5]
        self.base = base
        with open(base + ".json", encoding="utf-8") as f:
            self.meta = json.load(f)
        self.size = self.meta["size"]
        self.min_y = self.meta["world"]["minY"]
        self.height = self.meta["world"]["height"]
        d = os.path.dirname(base)
        self._blocks_path = os.path.join(d, self.meta["files"]["blocks"])
        self._ents_path = os.path.join(d, self.meta["files"]["entities"])

    # ---------- чтение потоков ----------
    def _block_events(self):
        """Генератор (тик, тип, (a, b, c), данные). Обрезанный gzip читается до места обрыва."""
        try:
            with gzip.open(self._blocks_path, "rb") as f:
                while True:
                    head = f.read(HEAD.size)
                    if len(head) < HEAD.size:
                        return
                    typ, tick, a, b, c = HEAD.unpack(head)
                    data = None
                    if typ == R_SECTION:
                        raw = f.read(4096 * 2)
                        if len(raw) < 4096 * 2:
                            return
                        data = np.frombuffer(raw, np.uint16).reshape(16, 16, 16).copy()
                    elif typ == R_BLOCK:
                        raw = f.read(2)
                        if len(raw) < 2:
                            return
                        data = struct.unpack("<H", raw)[0]
                    yield tick, typ, (a, b, c), data
        except (EOFError, OSError):
            return

    def _entity_lines(self):
        try:
            with gzip.open(self._ents_path, "rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        return
        except (EOFError, OSError):
            return

    # ---------- сборка кадров ----------
    def frames(self, step=1, start=0, stop=None):
        """Генератор (тик, origin, blocks[y][z][x], список сущностей) для тиков start..stop."""
        sections = {}
        origin = None
        ents = {}
        target = None

        bev = self._block_events()
        eev = self._entity_lines()
        nb = next(bev, None)
        ne = next(eev, None)
        last = self.meta.get("ticks", 0) - 1
        if stop is not None:
            last = stop

        t = 0
        while True:
            if stop is None:
                if t > last and nb is None and ne is None:
                    break
            elif t > stop:
                break

            while nb is not None and nb[0] <= t:
                _, typ, (a, b, c), data = nb
                if typ == R_SECTION:
                    sections[(a, b, c)] = data
                elif typ == R_DROP:
                    sections.pop((a, b, c), None)
                elif typ == R_ORIGIN:
                    origin = (a, b, c)
                elif typ == R_BLOCK:
                    sec = sections.get((a >> 4, b >> 4, c >> 4))
                    if sec is not None:
                        sec[b & 15, c & 15, a & 15] = data
                nb = next(bev, None)

            while ne is not None and ne["t"] <= t:
                if ne.get("full"):
                    ents.clear()
                for r in ne.get("upd", []):
                    ents[r["id"]] = r
                for i in ne.get("del", []):
                    ents.pop(i, None)
                if "tg" in ne:
                    target = ne["tg"]
                ne = next(eev, None)

            if t >= start and (t - start) % step == 0 and origin is not None:
                yield t, origin, self._crop(sections, origin), list(ents.values())
            t += 1

    def frame_at(self, tick):
        for item in self.frames(start=tick, stop=tick):
            return item
        return None

    def _crop(self, sections, origin):
        S = self.size
        x0, y0, z0 = origin
        cube = np.full((S, S, S), UNKNOWN, np.uint16)
        for sy in range(y0 >> 4, ((y0 + S - 1) >> 4) + 1):
            for sz in range(z0 >> 4, ((z0 + S - 1) >> 4) + 1):
                for sx in range(x0 >> 4, ((x0 + S - 1) >> 4) + 1):
                    sec = sections.get((sx, sy, sz))
                    if sec is None:
                        continue
                    wx0, wx1 = max(x0, sx * 16), min(x0 + S, sx * 16 + 16)
                    wy0, wy1 = max(y0, sy * 16), min(y0 + S, sy * 16 + 16)
                    wz0, wz1 = max(z0, sz * 16), min(z0 + S, sz * 16 + 16)
                    cube[wy0 - y0:wy1 - y0, wz0 - z0:wz1 - z0, wx0 - x0:wx1 - x0] = \
                        sec[wy0 - sy * 16:wy1 - sy * 16, wz0 - sz * 16:wz1 - sz * 16, wx0 - sx * 16:wx1 - sx * 16]
        # выше и ниже мира блоков нет — это воздух, а не «неизвестно»
        lo, hi = self.min_y, self.min_y + self.height - 1
        for dy in range(S):
            wy = y0 + dy
            if wy < lo or wy > hi:
                cube[dy] = 0
        return cube


def main():
    ap = argparse.ArgumentParser(description="CNN-DATA-BOT: сборка кадров из записи")
    ap.add_argument("base", help="путь к записи без расширения (или к .json)")
    ap.add_argument("--tick", type=int, help="собрать кадр на этом тике")
    ap.add_argument("--out", default="frame.npz", help="куда сохранить кадр (npz)")
    args = ap.parse_args()

    rec = Recording(args.base)
    m = rec.meta
    if args.tick is None:
        print(f"игрок:    {m['player']}")
        print(f"версия:   {m['version']['minecraft']}")
        print(f"старт:    {m['startedAt']}")
        print(f"куб:      {m['size']}^3, тиков: {m['ticks']} ({m['ticks'] * m['tickMs'] / 1000:.1f} с)")
        print(f"записей:  {m['counts']}")
        return

    item = rec.frame_at(args.tick)
    if item is None:
        raise SystemExit("кадр не найден (тик вне записи или нет ключевого кадра)")
    tick, origin, blocks, ents = item
    np.savez_compressed(args.out, blocks=blocks, origin=np.array(origin, np.int32),
                        tick=tick, entities=json.dumps(ents))
    print(f"тик {tick}, origin {origin}, блоков {blocks.shape}, сущностей {len(ents)} -> {args.out}")


if __name__ == "__main__":
    main()
