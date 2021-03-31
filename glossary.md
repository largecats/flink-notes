# Glossary

**Write-ahead logging (WAL).** A family of techniques for providing atomicity and durability in database systems. All changes are written to a log before they are applied.

**Replayable streaming source.** Data range of the last incomplete micro-batch can be re-read from source.

**Idempotent streaming sink.** The sink can identify re-executed micro-batches and ignore duplicate writes cased by restarts.