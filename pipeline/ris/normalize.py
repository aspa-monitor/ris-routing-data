"""AS_PATH parsing and cleaning.

Path order follows BGP: the leftmost ASN is the collector's peer, the rightmost is the origin.
For two neighbours "... L R ...", L received the route from R, so L is R's upstream-side
neighbour (a provider or a peer of R). These (R, L) pairs feed the ASPA "observed upstreams" view.

Input format (bgpkit): AS_SEQUENCE ASNs separated by spaces, AS_SET as "{a,b,c}",
confederation segments as "(a b)" or "[a,b]" (should never reach a collector over eBGP).
"""
from __future__ import annotations

from dataclasses import dataclass, field

# Flag names, in the order they are written to output.
FLAGS = ("as_set", "confed", "loop", "private", "reserved", "as_trans", "peer_mismatch", "empty")


def asn_category(asn: int) -> str | None:
    """Return 'private', 'reserved', 'as_trans' or None for a normal public ASN (IANA special registry)."""
    if asn == 23456:
        return "as_trans"
    if 64512 <= asn <= 65534 or 4200000000 <= asn <= 4294967294:
        return "private"
    if (asn == 0 or 64496 <= asn <= 64511 or asn == 65535 or 65536 <= asn <= 131071
            or asn == 4294967295):
        return "reserved"  # AS0, documentation, reserved blocks
    return None


@dataclass
class Segment:
    kind: str          # "seq", "set", "confed_seq", "confed_set"
    asns: list[int]


@dataclass
class CleanPath:
    raw: str
    tokens: list            # ints for sequence hops, tuple(sorted) for an AS_SET
    flags: set[str] = field(default_factory=set)
    origin: int | None = None
    set_members: tuple[int, ...] = ()
    adjacencies: list[tuple[int, int]] = field(default_factory=list)  # (asn, upstream_neighbour)

    @property
    def text(self) -> str:
        return " ".join("{" + ",".join(map(str, t)) + "}" if isinstance(t, tuple) else str(t)
                        for t in self.tokens)

    @property
    def length(self) -> int:
        """Path length after prepend collapse; an AS_SET counts as 1 hop (RFC 4271)."""
        return len(self.tokens)


def parse_segments(raw: str) -> list[Segment]:
    segments: list[Segment] = []
    seq: list[int] = []
    i, s = 0, raw.strip()
    closers = {"{": ("}", "set"), "(": (")", "confed_seq"), "[": ("]", "confed_set")}
    while i < len(s):
        c = s[i]
        if c.isspace():
            i += 1
        elif c in closers:
            end_char, kind = closers[c]
            j = s.index(end_char, i)
            if seq:
                segments.append(Segment("seq", seq))
                seq = []
            members = [int(x) for x in s[i + 1:j].replace(",", " ").split()]
            segments.append(Segment(kind, members))
            i = j + 1
        else:
            j = i
            while j < len(s) and not s[j].isspace() and s[j] not in closers:
                j += 1
            seq.append(int(s[i:j]))
            i = j
    if seq:
        segments.append(Segment("seq", seq))
    return segments


def normalize(raw: str | None, peer_asn: int | None = None) -> CleanPath:
    raw = (raw or "").strip()
    cp = CleanPath(raw=raw, tokens=[])
    if not raw:
        cp.flags.add("empty")
        return cp

    set_members: list[int] = []
    for seg in parse_segments(raw):
        if seg.kind.startswith("confed"):
            cp.flags.add("confed")
            continue  # confed segments are not counted in path length and are not inter-AS hops
        if seg.kind == "set":
            cp.flags.add("as_set")
            members = tuple(sorted(set(seg.asns)))
            set_members.extend(members)
            cp.tokens.append(members)
            continue
        for asn in seg.asns:
            if cp.tokens and cp.tokens[-1] == asn:
                continue  # prepending
            cp.tokens.append(asn)

    seq_asns = [t for t in cp.tokens if isinstance(t, int)]
    if len(seq_asns) != len(set(seq_asns)):
        cp.flags.add("loop")
    for asn in seq_asns + set_members:
        cat = asn_category(asn)
        if cat:
            cp.flags.add(cat)
    if peer_asn is not None and (not cp.tokens or cp.tokens[0] != peer_asn):
        cp.flags.add("peer_mismatch")

    cp.set_members = tuple(sorted(set(set_members)))
    if cp.tokens and isinstance(cp.tokens[-1], int):
        cp.origin = cp.tokens[-1]

    # Adjacencies: only from the ordered part, only between public ASNs, never from looped paths.
    if "loop" not in cp.flags:
        for left, right in zip(cp.tokens, cp.tokens[1:]):
            if isinstance(left, int) and isinstance(right, int) \
                    and asn_category(left) is None and asn_category(right) is None:
                cp.adjacencies.append((right, left))
    return cp
