"""Frozen signed-baseline traces protect the standalone staging extraction."""

import hashlib
import json
import random

import pytest

import stream_quilt.window_fold as current

BASELINE_COMMIT = "58eeb22be2da770f7226ccf778de2099a1ba2bd8"
BASELINE_SOURCE_SHA256 = "dc5e4ae4cae3c9a3ae49f23d55f24cb0dbdda50ee7e8315cdb04f4bc0d1b2e9e"
BASELINE_DIGESTS = {
    '[2,5,1,"drop",false,31]': {
        "bytes": 40934,
        "operations": 37,
        "sha256": "583edd531cfc1193932afd182ad2e5e06e70a33ab9140c37bb3d0545b836d91a",
    },
    '[2,5,1,"drop",false,7]': {
        "bytes": 48212,
        "operations": 40,
        "sha256": "61b97cbf6278fffee6dc23c840858770f4c3cfa9b39dc6330e2059986608b585",
    },
    '[2,5,1,"drop",true,31]': {
        "bytes": 41335,
        "operations": 37,
        "sha256": "a50b289dc257eb0536b67946185fce9de6554fe8899c9406de2b93e36a812a20",
    },
    '[2,5,1,"drop",true,7]': {
        "bytes": 49028,
        "operations": 40,
        "sha256": "80ddcf2d81d1cb6f376249423839f855fbe4c550ea5244abcd99938a6405952e",
    },
    '[2,5,1,"reject",false,31]': {
        "bytes": 40687,
        "operations": 37,
        "sha256": "f047aa1a53dcc0c8074d3b2bc45c362d253f43f60dff97debd9d82100f724d29",
    },
    '[2,5,1,"reject",false,7]': {
        "bytes": 48020,
        "operations": 40,
        "sha256": "480c5cacda9469e9845358d1d27f9b7ee5394406cd019d3d701e828c0c3fb2b9",
    },
    '[2,5,1,"reject",true,31]': {
        "bytes": 41088,
        "operations": 37,
        "sha256": "c579f63336cb93b4ccb759e5eebed0e7f031b60e51cca483f4ddef8a78168299",
    },
    '[2,5,1,"reject",true,7]': {
        "bytes": 48836,
        "operations": 40,
        "sha256": "e70bf9903d20a772195bafa5d9c6aa26b3447b689886b12c4d418c6f1a25c5cd",
    },
    '[5,5,0,"drop",false,31]': {
        "bytes": 73434,
        "operations": 48,
        "sha256": "c7bb0b75487900907ac1c262edc5bcb8e5f053b158778534f601961ca9674e51",
    },
    '[5,5,0,"drop",false,7]': {
        "bytes": 75584,
        "operations": 48,
        "sha256": "b488300d4da0f01f7229e742146c0603ef0414d26644f6d1e507765f087e43df",
    },
    '[5,5,0,"drop",true,31]': {
        "bytes": 77160,
        "operations": 48,
        "sha256": "3e3b0b47226945cfefe36cb9b3c11e610a914b9d3592b35b2fb74185859ac7f4",
    },
    '[5,5,0,"drop",true,7]': {
        "bytes": 79776,
        "operations": 48,
        "sha256": "cbb2d38993b3977024e35f7562fa31cd0487d204900c54d4b19ed5250b710834",
    },
    '[5,5,0,"reject",false,31]': {
        "bytes": 73208,
        "operations": 48,
        "sha256": "3be46bc2ce7c40ba510bfff32419a73bc86c0e259ecdcf0aba5e8d7b233e2b6f",
    },
    '[5,5,0,"reject",false,7]': {
        "bytes": 75315,
        "operations": 48,
        "sha256": "315933cef678f4a74a42ee51f09e7a14ea333d57ba6d9dabb63dff4432931738",
    },
    '[5,5,0,"reject",true,31]': {
        "bytes": 76934,
        "operations": 48,
        "sha256": "b153520351ff02a3b95c93d0253e3beed454860db6f8dd630102d08c83b19b51",
    },
    '[5,5,0,"reject",true,7]': {
        "bytes": 79507,
        "operations": 48,
        "sha256": "27279c853c7c393a668dc91bef85ec77dc5df8b689ffefcc4649436650d7df07",
    },
    '[7,3,-2,"drop",false,31]': {
        "bytes": 114374,
        "operations": 52,
        "sha256": "a3ee0187ed14c6ee9f1a89937148ead3d15cda31e4c01231baa245d0b1e822c5",
    },
    '[7,3,-2,"drop",false,7]': {
        "bytes": 114131,
        "operations": 53,
        "sha256": "a3e58267398ee3c938c896e56dceebc36d73808f1b1adc27bce7e78d5fc184bd",
    },
    '[7,3,-2,"drop",true,31]': {
        "bytes": 122128,
        "operations": 52,
        "sha256": "cd7f08ad8e8463013d0c212ef326d0460365cf892e907396a3483d18b339aa02",
    },
    '[7,3,-2,"drop",true,7]': {
        "bytes": 122137,
        "operations": 53,
        "sha256": "0024d7f18c774e3b5d6515c8409782a50cd5673dabf0ea3dbe398d5eb574f6c3",
    },
    '[7,3,-2,"reject",false,31]': {
        "bytes": 114113,
        "operations": 52,
        "sha256": "1051bfd388816f16cf218325c65775a3fc3ebca8f6a32d83012f964cb4a02cd0",
    },
    '[7,3,-2,"reject",false,7]': {
        "bytes": 113871,
        "operations": 53,
        "sha256": "58e5d15b8d8b6866fd6a7387dbe098fbf910192c2c92649a7bfe78b3851b35a7",
    },
    '[7,3,-2,"reject",true,31]': {
        "bytes": 121867,
        "operations": 52,
        "sha256": "b52f274bcca72ea02916498cec6da7652289235ce2544d1c654c2b4d97682c16",
    },
    '[7,3,-2,"reject",true,7]': {
        "bytes": 121877,
        "operations": 53,
        "sha256": "91bd550ce784e68a112b3451cb229a85ade279bd77cc8b12402b47fca44ed7a1",
    },
}


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def capture_trace(module, case):
    width, hop, origin, policy, finalizer, seed = case
    callbacks, report = [], []

    def initial():
        callbacks.append(["initial"])
        return []

    def fold(state, value):
        callbacks.append(["fold", list(state), value])
        return [*state, value]

    def finalize(state):
        callbacks.append(["finalize", list(state)])
        return {"total": sum(state), "arrivals": list(state)}

    spec = module.WindowFold(
        "baseline",
        "v1",
        width=width,
        hop=hop,
        origin=origin,
        initial=initial,
        fold=fold,
        finalize=finalize if finalizer else None,
        late_policy=policy,
        limits=module.WindowFoldLimits(max_row_bytes=512, max_batch_bytes=1024),
    )
    runtime = module.WindowFoldRuntime(spec)

    def operation(name, *args):
        nonlocal runtime
        try:
            if name == "process":
                result = runtime.process(args[0], module.FlowRecord(args[1], args[2]))
                value = [result.outcome, result.memberships, result.status.to_dict()]
            elif name == "advance":
                value = runtime.advance_watermark(args[0]).to_dict()
            elif name == "finish":
                value = runtime.finish().to_dict()
            else:
                result = runtime.drain(max_windows=args[0])
                value = [[row.to_dict() for row in result.rows], result.status.to_dict()]
            outcome = ["ok", value]
        except BaseException as error:
            outcome = [
                "error",
                type(error).__name__,
                str(error),
                type(error.__cause__).__name__ if error.__cause__ is not None else None,
            ]
        point = runtime.checkpoint()
        report.append([name, args, outcome, list(callbacks), point.to_json()])
        runtime = module.WindowFoldRuntime.from_checkpoint(
            spec, module.WindowCheckpoint.from_json(point.to_json())
        )

    rng = random.Random(seed)
    for index in range(20):
        operation("process", rng.randrange(-15, 20), rng.randrange(-8, 9), rng.choice(["a", "é"]))
        if index % 5 == 4:
            operation("advance", [-10, 0, 10, 30][index // 5])
            if runtime.status.phase == "draining":
                operation("process", 99, 1, "a")
                operation("advance", 99)
            while runtime.status.phase == "draining":
                operation("drain", rng.randrange(1, 4))
    operation("advance", -100)
    operation("process", True, 1, "a")
    operation("process", 2**53, 1, "a")
    operation("process", 31, 1, None)
    operation("finish")
    while runtime.status.phase == "draining":
        operation("drain", 1)
    operation("finish")
    operation("drain", 1)
    operation("drain", 0)
    operation("process", 40, 1, "a")
    operation("advance", 40)
    encoded = canonical(report).encode()
    return {
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "operations": len(report),
        "bytes": len(encoded),
    }


CASES = [
    (*geometry, policy, finalizer, seed)
    for geometry in [(5, 5, 0), (7, 3, -2), (2, 5, 1)]
    for policy in ("reject", "drop")
    for finalizer in (False, True)
    for seed in (7, 31)
]


@pytest.mark.parametrize("case", CASES)
def test_full_standalone_traces_match_signed_baseline(case):
    assert capture_trace(current, case) == BASELINE_DIGESTS[canonical(case)]
