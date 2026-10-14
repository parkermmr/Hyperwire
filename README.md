[![CI Status][ci-shield]][ci-url]
[![Coverage][coverage-shield]][coverage-url]
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]

# Hyperwire

Async event pipelines wired with `>>`. Every node has a bounded inbox
and its own pool of workers, so backpressure and parallelism are built
into every stage. Graphs can branch on predicates, share state, retry
and route failures, and hand individual stages to remote compute such
as a process pool or a Ray cluster.

```
pip install hyperwire                # core, files and codecs, standard library only
pip install "hyperwire[kafka]"       # + kafka sources, sinks and claim checks (plyght[kafka])
pip install "hyperwire[s3]"          # + claim checks parked in s3 (plyght[s3])
pip install "hyperwire[all]"
```

## Usage

```python
import asyncio
from hyperwire import Pipeline, otherwise, processor, when
from hyperwire.files import JsonlSink, JsonlSource

@processor
def classify(message):
    message.metadata["lane"] = "fast" if message.content.get("urgent") else "slow"
    return message

@processor(workers=4)
async def enrich(message, state):
    message.content["seen"] = await state.increment("seen")
    return message

@processor
def archive(message):
    return message

source = JsonlSource("./in.jsonl")
sink = JsonlSink("./out.jsonl")
source >> classify >> [when(lane="fast") >> enrich, otherwise >> archive] >> sink

asyncio.run(Pipeline(name="demo").run(source))
```

`@processor` needs no pipeline. Wiring a processor with `>>` makes it
part of whichever pipeline runs the graph, and that pipeline supplies
its state, context and error route when the run starts. A processor is
still callable as the plain function, so `classify(message)` works in a
unit test.

## How a pipeline runs

- **Messages.** `Message(content, metadata=None, id=None, timestamp=None)`.
  Sources create them, sinks deliver them, and every node in between
  transforms them. A handler that returns `None` drops the message.
- **Backpressure.** Each inbox holds 1024 messages by default. Emitting
  into a full inbox blocks the sender, so a slow node slows everything
  upstream of it instead of filling memory. `pipeline.pause()` stops
  consumption between messages, and the backlog builds up upstream in
  the same way.
- **Parallelism.** `workers=8` runs eight handlers on one node at once,
  so ordering across that node is not guaranteed. Sync functions run in
  a thread so they never block the event loop. `mode="process"` gives
  each worker its own process, which needs a picklable function of the
  message alone.
- **Lifecycle.** `pipeline.run(source)` starts every node, pumps the
  source until it is exhausted, waits for every inbox to drain, commits
  the source, then stops the graph in order. A long-lived source, such as
  Kafka or a watched directory, runs until it is closed or
  `pipeline.stop()` is called. A run that errors never commits.

## Graphs

| Construct                                   | Meaning                                                             |
|---------------------------------------------|---------------------------------------------------------------------|
| `a >> b >> c`                               | a chain                                                             |
| `a >> [b, c] >> d`                          | fan out to both, converge on `d`; each arm gets its own copy          |
| `a >> [when(kind="x") >> b, otherwise >> c]` | `when` matches metadata keywords or takes a predicate; `otherwise` fires when nothing matched |
| `[... , branch >> terminate]`               | an arm that does not converge                                       |
| `component(a, b, c, name="clean")`          | a reusable section, connectable like a node, scaled as one          |
| `Publisher(["done"])` / `Listener(["done"])` | in process topics that join separate graph lines                    |
| `Reservoir(limit=10_000, overflow="block")` | holds messages until released: `open()`, `release(n)`, `drop(n)`, `peek()` |
| `pipeline.assemble(a, b, c)`                | the same as `>>`, without operators                                  |

`pipeline.describe()` returns a serializable snapshot of the graph,
with every node's counters, edges and component nesting.

## Processors

```python
@processor(name="price", workers=2, retries=3, backoff=0.5, errors=dead_letters)
async def price(message, state, context):
    ...
```

- A handler that declares a `state` parameter receives the pipeline's
  `State`. One that declares `context` receives the pipeline's
  `Context`, a holder for clients and configuration that messages should
  not carry (`context.db`, `context.resolve(Database)`).
- `retries` re-runs a failing handler with exponential backoff (doubling,
  capped at 30 seconds). Only after the last attempt does the message go
  to the error route, which is the node's `errors=` or the pipeline's
  `Pipeline(errors=...)`.
- `Pipeline(state=MemoryState())` is the default. `FileState(directory)`
  persists across runs. Both support `get`, `set(key, value, ttl=None)`,
  `delete`, `keys(pattern)`, `increment` and `lock(key)`. Expired keys
  are dropped when read, and `sweep=` adds a watchdog that purges them in
  the background.
- `pipeline.performance()` copies fan out messages shallowly and runs
  sync handlers on a wide shared thread pool.
  `performance(experimental=True)` is meant for free threaded Python
  builds.

## Remote stages

```python
from hyperwire import Pipeline, remote

@remote(cpus=2.0, ttl=120, workers=8)
def score(message):
    message.content["score"] = expensive(message.content)
    return message

pipeline = Pipeline(name="scored", compute=compute)
source >> score >> sink
```

A remote stage submits one named job per message to a compute and
emits each result as soon as its job finishes. Any object with this
shape is a compute:

```python
async def submit(function, name, arguments=(), keywords=None, cpus=1.0, ttl=3600.0) -> bool
async def poll(name) -> result | None    # result has .value, .error and .successful()
```

[asteroidbelt](https://github.com/parkermmr/asteroidbelt) provides
`LocalCompute`, `ThreadCompute` and `RayCompute`, and hyperwire does not
import it.

- The compute is chosen when the run starts. A node uses its own
  (`remote(compute=...)` or `score.compute(...)`) if it has one, then the
  pipeline's. With neither, the function runs in process like any other
  processor, so the same graph works with or without a cluster.
- Job names come from message ids, so a redelivered message never runs
  the same work twice. `limit` (default 32) caps how many jobs are in
  flight, which is the node's backpressure.
- A remote function receives only the message. It may run on another
  machine, so it cannot use pipeline state or closures over live
  objects, and the message content must survive serialization.

## Sources and sinks

| Transport                          | Install    | Direction | What it does                                                          |
|------------------------------------|------------|-----------|-----------------------------------------------------------------------|
| `QueueSource` / `MemorySink`       | core       | in / out  | push messages in by hand; collect them in a list                       |
| `hyperwire.files.JsonlSource`      | core       | in        | one message per line                                                  |
| `hyperwire.files.CsvSource`        | core       | in        | one message per row                                                   |
| `hyperwire.files.DirectorySource`  | core       | in        | watch a directory; one message per file once its size and mtime settle |
| `hyperwire.files.JsonlSink`        | core       | out       | one json line per message; truncated when the run starts               |
| `hyperwire.kafka.KafkaSource`      | `[kafka]`  | in        | a consumer group member on one topic                                  |
| `hyperwire.kafka.KafkaSink`        | `[kafka]`  | out       | produce each message keyed by its id, waiting for the ack              |
| `hyperwire.kafka.ClaimCheckSink`   | `[kafka]`, `[s3]` for buckets | out | park the payload in s3 or a directory, and queue a small json claim |
| `hyperwire.kafka.ClaimCheckSource` | `[kafka]`  | in        | redeem claims back into messages                                      |

Kafka and s3 settings come from `PLYGHT_KAFKA_*` and `PLYGHT_S3_*`
(see [plyght](https://github.com/parkermmr/plyght)) unless they are
passed in. Keyword overrides, such as `KafkaSource("events", bootstrap="broker:9092")`,
take precedence.

### Codecs

Anything that crosses a byte boundary arrives as `bytes` unless you
name a codec. This includes a Kafka value, a parked claim check payload
and a file read with `read=True`. Kafka Connect and most producers do
not send JSON, so nothing guesses the format.

| Codec      | Content                                                                 |
|------------|-------------------------------------------------------------------------|
| `bytes`    | default; the payload as it is                                           |
| `text`     | utf-8 text                                                              |
| `json`     | parsed JSON                                                             |
| `envelope` | the whole message (id, metadata, timestamp, content), for flow to flow hops |

Claim check claims are the one place JSON is assumed, because the claim
format belongs to hyperwire. Each claim records the codec its payload
was parked with, so the source decodes it the same way.
`hyperwire.codecs.register(codec)` adds your own codec, which is any
object with `name`, `suffix`, `encode(message)` and
`decode(payload, metadata)`.

## Observing

- `pipeline.provenance(Provenance(depth=50, width=4096))` keeps a
  bounded ring of recent messages per node, with payloads truncated to
  `width`. `read(node, view="application/json")` renders the records.
  Recording can be switched on and off while the graph runs, and it never
  backpressures the flow.
- `sys.stdout = TaggedStream(sys.stdout)` (from `hyperwire.runtime.tagged_stream`) prefixes every line printed
  inside a node's worker with `[node:<name>]`.
- Every node counts `processed`, `failed`, `retried` and `last_error`.
  Sources report `lag()` and `partitions()` when their broker can tell.

## Behaviour notes

- Fan out arms receive deep copies, or shallow copies in performance
  mode, so parallel branches cannot interfere with each other.
- A `Reservoir` holding messages is never idle, so a run does not finish
  and its source does not commit while anything is held.
- `KafkaSource` commits through the consumer's auto commit. An offset
  can therefore be committed before its message has left the graph, and
  a crash can skip those messages. Use claim checks or idempotent sinks
  where that matters.
- `DirectorySource` emits the file's coordinates (`path`, `name`,
  `size`, `modified`) by default. With `read=True` it emits the file's
  bytes and moves the coordinates to metadata.

## Samples

- [`sample/example.py`](sample/example.py) - routing with parallel
  workers, state with a context and an error route, and jsonl files.
- [`sample/remote.py`](sample/remote.py) - a remote stage on asteroidbelt's
  `LocalCompute`.

## Tests

```
pip install -e ".[all,dev]"
pytest --cov
```

[ci-shield]: https://img.shields.io/github/actions/workflow/status/parkermmr/hyperwire/ci.yml?branch=main&style=for-the-badge&label=CI
[ci-url]: https://github.com/parkermmr/hyperwire/actions/workflows/ci.yml
[coverage-shield]: https://img.shields.io/codecov/c/github/parkermmr/hyperwire?style=for-the-badge
[coverage-url]: https://codecov.io/gh/parkermmr/hyperwire
[contributors-shield]: https://img.shields.io/github/contributors/parkermmr/hyperwire.svg?style=for-the-badge
[contributors-url]: https://github.com/parkermmr/hyperwire/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/parkermmr/hyperwire.svg?style=for-the-badge
[forks-url]: https://github.com/parkermmr/hyperwire/network/members
[stars-shield]: https://img.shields.io/github/stars/parkermmr/hyperwire.svg?style=for-the-badge
[stars-url]: https://github.com/parkermmr/hyperwire/stargazers
[issues-shield]: https://img.shields.io/github/issues/parkermmr/hyperwire.svg?style=for-the-badge
[issues-url]: https://github.com/parkermmr/hyperwire/issues
