# Blogging platform data generation

The project generates different datasets that you can use to test your data engineering applications. It simulates
real-life data quality issues such as late, incomplete, or duplicate data.

## Components

Data generation relies on different layers: entities, data blockers, data generators, and writers. 

### Entities
![entities_layer.png](assets%2Fentities_layer.png)

This layer is responsible for:

* defining the entities content (`model`); typically these are `@dataclass`es describing a given entity
* creating the individual entity instances (`generator`)

### Dataset generators
![dataset_generation_layer.png](assets%2Fdataset_generation_layer.png)

Dataset generators control the dataset generation process, i.e. when the application should stop creating the given dataset.
There are currently 3 supported controllers:

* One-shot: generates the dataset only once
* Fixed times: runs the generation loop a given number of times
* Continuous: runs until interrupted by the user; typically useful for data generation for streaming jobs

### Dataset generation blockers
![dataset_generation_layer.png](assets%2Fdataset_generation_layer.png)

Objects from this layer control the dataset generation rate, i.e. whether the generation job should pause between
2 generation loops

There are currently 2 supported blockers:
* None: there is no pause between the loops.
* Sleeping: there is a pause of x to y seconds; final value is generated randomly from the input each time

### Writers
![writer_classes.png](assets%2Fwriter_classes.png)

This layer exposes the API for writing the generated rows to the supported data stores.

## Data generation loop

The data generation process is in the [main_generator.py](data_generator%2Fdatasets%2Fmain_generator.py) file:
```python
def generate_dataset(generator: DatasetGenerator, context: DatasetGenerationContext,
                     writer: DatasetWriter):
    rows_to_generate = context.get_rows_to_generate_with_maybe_decorators()
    while generator.should_continue():
        for row_decorator in rows_to_generate:
            row = row_decorator.return_decorated_row()
            logging.debug(f'Generated row is {row}')
            writer.write_dataset_decorated_rows(row)
        writer.flush()

        context.irregular_data_blocker.block()
```

The snippet shows all the components presented in the previous section. All starts with the generation controller 
called in the `while` loop. Later, the `return_decorated_row` invokes the entity generator while all the `writer` occurrences
the sinks from the writer layer. In the end, the `irregular_data_blocker` might eventually stop the data generation by 
calling the configured data blocker.

## Configuration

To use the project, you can either adapt one of th examples present in the [dataset_generator_examples](dataset_generator_examples)
or use the Docker image. 

### Docker image
Using the Docker image requires creating a configuration `YAML` file following the schema
```yaml
dataset:
  rows: x # number of rows to generate
  composition_percentage:
    duplicates: x # % of duplicates
    missing_fields: x # % of rows with missing fields
    unprocessable_rows: x # % of rows with unprocessable formats
data_blocker:
  type: x # type of the data blocker
entity:
  type: x # type of the generated entity w/ an optional configuration
  configuration:
    property1: x
generator:
  type: x # type of the data generator
writer:
  type: x # type of the dataset writer w/ an optional configuration
  configuration:
    property1: x
  partitions: [...] # it's an optional list of partitions, e.g. for JSON file writer
```

Since some of the attributes accept complex types, below you can find a more detailed configuration for them.

#### Data blocker - None
```yaml
data_blocker:
  type: 'no''
```
#### Data blocker - Sleeping
```yaml
data_blocker:
  type: sleep
  configuration:
    sleep_time_range_seconds:
      from: 2
      to: 6   
```

### Entity - devices or users
```yaml
entity:
  type: 'device'
```
```yaml
entity:
  type: 'user'
```

### Entity - visits
```yaml
entity:
  type: visit
  configuration:
    start_time: '2023-11-01T00:00:00Z'
```
_start_time_ defines when the visits start.

### Generator - one-shot or continuous
```yaml
generator:
  type: one-shot
```
```yaml
generator:
  type: continuous
```

### Generator - fixed times
```yaml
generator:
  type: fixed-times
  configuration:
    all_runs: 5
```
_all_runs_ defines how many times the generation loop should run before quitting

### Writer - Apache Kafka
```yaml
writer:
  type: kafka
  configuration:
    broker: 'localhost:9094'
    output_topic: 'visits'
    extra_producer_config:
      'queue.buffering.max.ms': 2000
```

_extra_producer_config_ sets the extra configuration to pass to the Kafka Producer. The attributes must be accepted by the
`confluent_kafka.cimpl.Producer` class.

### Writer - file system CSV
```yaml
writer:
  type: csv
  configuration:
    output_path: '/home/data_generator_user/data_generator_output/input'
    clean_path: true
  partitions: ['date=2023-11-01', 'date=2023-11-02', 'date=2023-11-03', 'date=2023-11-04',
               'date=2023-11-05', 'date=2023-11-06', 'date=2023-11-07']
```
_output_path_ defines where the data should be written. Remember, it's the location on the Docker container and not
your localhost. If you mount the volumes, **create the mounted directories on your file system first**. Otherwise, 
you may encounter permission issues

_clean_path_ defines whether the writer should delete all files from the output directory before writing the new ones.

_partitions_ defines a list of partitions to write data to. Each partition will be generated from a new 
data generation loop. As a result, if you configured a fixed-size data generator of 5 runs, there will be 5 generation
loops against each partition. **The partitions are not related to the entities**! They're optional. If you don't
define this attribute, data will be written to the _output_path_ directly. Otherwise, the writer will create
files under _${output_path}/${partition}_.
 
### Writer - file system JSON
```yaml
writer:
  type: csv
  configuration:
    output_path: '/home/data_generator_user/data_generator_output/input'
    clean_path: true
  partitions: ['date=2023-11-01', 'date=2023-11-02', 'date=2023-11-03', 'date=2023-11-04',
               'date=2023-11-05', 'date=2023-11-06', 'date=2023-11-07']
```


### Writer - PostgreSQL
```yaml
writer:
  type: postgresql
  configuration:
    host: 'postgres'
    dbname: 'dedp'
    db_schema: 'dedp_schema'
    user: 'postgres'
    password: 'postgres'
    table_name: 'visits'
    table_columns: ['visit_id', 'event_time']
    row_fields_to_insert: ['visitId', 'eventTime']
```

_table_columns_ defines the list of columns in the output table.
_row_fields_to_insert_ maps the attributes of the generated entity to the columns. In the example, the _visitId_ attribute
is mapped to the _visit_id_ column and the _eventTime_ to the _event_time_.

# Test
## PyCharm
To launch the tests on PyCharm, you need to enable pytest as the test runner for the project. You can see how to do this
on [jetbrains.com page](https://www.jetbrains.com/help/pycharm/pytest.html)

## Command line
To execute all tests from command line, you can use `make test_all` command. To check test coverage, you can execute
`make test_coverage`.

# Development
## virtualenv
Setup a virtualenv environment:
```
virtualenv -p python3 .venv/
```

Activate the installed environment:
```
source .venv/bin/activate
```

Install dependencies (venv activated):
``` 
pip3 install -r requirements.txt
```

Desactivate the virtualenv:
```
deactivate
```
## Code checks
Check code format:
```
make lint_all
```

Reformat code:
```
make reformat_all
```

## Pre-commit hook setup
The hook will execute the code formatting before the commit and the unit tests before the push. To install
it, please use [Pre-commit plugin](https://pre-commit.com/) and `pre-commit install` command.