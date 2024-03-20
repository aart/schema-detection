## Design

The technical design is visualized in the design document (design.pdf).

### Key Design Principles:
- Performance through concurrency
- Big data by supporting splitted input files
- Enforce consistency checks upstream
- Fail fast in case of an error
- File position traceback to enable debugging

### Constraints:
- A process will generate one schema. To enable generation of different schemas seperate processes need to be instantiated.

### Not supported yet:
- Repeated records
- Constraint relaxation for the "Required" attribute 
- No handling for the JSON string literal "null"
- Distribution
- Schema inference by parsing through repeated records
- Integration with Google Cloud (Cloud Storage, Bigquery, Dataflow)
- Incomplete support for string-wrapped types: Timestamp, Time, Date
- Test automation 
- Sampling instead of full file processing
- Structure in seperate packages
- Command line interface