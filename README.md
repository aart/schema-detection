## Design

The technical design is visualized in the design document (design.pdf).

## Design Principles:
- Performance through concurrency
- Enforce consistency checks upstream
- Fail fast in case of an error
- Traceback to enable debugging

## Constraints:
- A process will generate one schema. To enable generation of different schemas seperate processes need to be instantiated.

### Not supported:
- No handling for the JSON string literal "null".  
- Distribution
