# Glossary

**Write-ahead logging (WAL).** A family of techniques for providing atomicity and durability in database systems. All changes are written to a log before they are applied.

**Replayable streaming source.** Data range of the last incomplete micro-batch can be re-read from source.

**Idempotent streaming sink.** The sink can identify re-executed micro-batches and ignore duplicate writes cased by restarts.

**Window**. A grouping based on time. Splits the stream into buckets of finite size.

**Tumble window.** A window with fixed size and whose buckets do not overlap.

**POJO.** A plain old Java object is an ordinary Java object, not bound by any special restriction.

**Single Abstract Method (SAM) pattern.** Java interfaces (traits in Scala) that allow only one abstract method.