## Design

The technical design of the core is visualized in the design blueprint (design.pdf).

### Key Design Principles:
- Performance through concurrency (using golang channel primitives)
- Big data supporting through splitted input files
- Enforce consistency checks upstream
- Fail fast in case of an error
- File position traceback to enable debugging

### Constraints:
- A process will generate one schema. To enable generation of different schemas seperate processes need to be instantiated.

### Features:
- Recursively nested and repeated fields
- Core functionality is structured in a reusable packages
- Command line interface (CLI) enabling configurability
- Single binary executable. Should play well together with Google CLI tools like gcloud and bq.


### Not supported yet:
- Constraint relaxation for the "Required" attribute 
- No handling for the JSON string literal "null"
- Distribution or clustered deployment
- Schema inference by parsing through repeated records (now the nested schema is based on the first element)
- API integration with Google Cloud (Cloud Storage, Bigquery, Dataflow)
- Incomplete support for string-wrapped types: Timestamp, Time, Date, Geo-types, ...
- Test automation 
- Sampling instead of comprehensive line-by-line processing
