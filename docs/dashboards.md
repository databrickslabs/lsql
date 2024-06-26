# Dashboards as code

<!-- TOC -->
* [Dashboards as code](#dashboards-as-code)
* [Building blocks](#building-blocks)
  * [`.sql` files](#sql-files)
    * [Metadata](#metadata)
    * [Headers of SQL files](#headers-of-sql-files)
      * [Widget arguments](#widget-arguments)
    * [Implicit detection](#implicit-detection)
    * [Widget types](#widget-types)
    * [Widget ordering](#widget-ordering)
    * [Widget identifiers](#widget-identifiers)
    * [Database name replacement](#database-name-replacement)
    * [Overrides](#overrides)
  * [`.md` files](#md-files)
    * [Text widget arguments](#text-widget-arguments)
  * [`dashboard.yml` file](#dashboardyml-file)
  * [Using as library](#using-as-library)
  * [Configuration precedence](#configuration-precedence)
* [Command-line interface](#command-line-interface)
<!-- TOC -->


Dashboards as code is a way to define dashboards in a declarative way, using a configuration file. 
This allows you to manage your dashboards in a version control system, and apply changes to your dashboards in 
a more controlled way. 

# Building blocks

Dashboards can be defined from `.sql`, `.md` and `dashboard.yml` files, and are structured in a way that is easy to read and
write. Here's the example of a folder that defines a dashboard:

```text
├── assessment
│   ├── azure
│   │   └── 05_0_azure_service_principals.sql
│   ├── main
│   │   ├── 00_0_metastore_assignment.md
│   │   ├── 00_4_is_incompatible_submit_run_detected.sql
│   │   ├── 01_0_group_migration.md
│   │   ├── 01_2_group_migration.sql
│   │   ├── 01_5_group_migration_complexity.sql
│   │   ├── 02_0_data_modeling.md
│   │   ├── 02_2_uc_data_modeling.sql
│   │   ├── 02_5_uc_data_modeling_complexity.sql
│   │   ├── 03_0_data_migration.md
│   │   ├── 03_2_data_migration_summary.sql
│   │   └── 03_5_data_migration_complexity.sql
...
```

[[back to top](#dashboards-as-code)]

## `.sql` files

SQL files are used to define the queries that will be used to populate the dashboard:

```sql
/* --width 2 --height 6 --order 0 */
WITH raw AS (
  SELECT object_type, object_id, IF(failures == '[]', 1, 0) AS ready 
  FROM $inventory.objects
)
SELECT CONCAT(ROUND(SUM(ready) / COUNT(*) * 100, 1), '%') AS readiness FROM raw
```

Name of the file has special meaning, as it is used to determine the [order of the tiles](#widget-ordering) in the dashboard.

[[back to top](#dashboards-as-code)]

### Metadata

Most of the metadata could be [inferred from the query](#implicit-detection) itself, but sometimes you need to specify it explicitly either 
within the [query itself](#headers-of-sql-files) or in the [`dashboard.yml` file](#dashboardyml-file). The main principle for the metadata we define
in the SQL files is to keep the structure of it as flat as possible, while still allowing to add [`overrides` for the cases](#overrides)
when the metadata cannot be inferred from the query itself.

[[back to top](#dashboards-as-code)]

### Headers of SQL files

The header is used to define widget and vizualization metadata used to render the relevant portion of the dashboard.
Metadata could be defined in a `--` or `/* ... */` comment, which are detected by our SQL parser. The parser only reads
the **comment starting on the top**, which can be a single line using `--` or span multiple lines
using `/* ... */`.

| Format       | Readability | Verbosity |
|--------------|-------------|-----------|
| YAML         | ?           | lower     |
| JSON         | ?           | higher    |
| `argparse`   | ?           | lowest    |
| Query string | ? | ? |

#### Widget arguments

The following widget arguments are supported:

| Flag                | Description                                 | Type  | Optional |
|---------------------|---------------------------------------------|-------|----------|
| --id                | The widget identifier                       | str   | Yes      |
| -o or --order       | The order of the widget                     | int   | Yes      |
| -w or --width       | The number of columns that the widget spans | int   | Yes      |
| -h or --height      | The number of rows that the widget spans    | int   | Yes      |
| -t or --title       | The widget title                            | str   | Yes      |
| -d or --description | The widget description                      | str   | Yes      |
| --type              | The widget type                             | str   | Yes      |
| -f or --filter      | The column(s) used when filtering           | str   | Yes      |

[[back to top](#dashboards-as-code)]

### Implicit detection

We aim at inferring the most of the metadata possible for the query and widget, so you can focus on the query itself.

We rely on the SQL parser to infer the metadata from the query itself. We may infer the following metadata 
from the query:

* column names
* widget types

[[back to top](#dashboards-as-code)]

### Widget types

The aim of this project is to support high-level metadata for a subset of the Databricks Lakeview widgets, while still
allowing to define the rest of the metadata in the `dashboard.yml` file. The following widget simplified types are 
supported:

- `counter`
- `table`
- [`text`](#md-files)

[[back to top](#dashboards-as-code)]

### Widget ordering

The order of the tiles in the dashboard is determined by the order of the SQL files in the folder, order of `tiles` 
in the [`dashboard.yml` file](#dashboardyml-file), or by the `order` key in the [SQL file metadata](#metadata).

The ordering would also be based on the width and height of the widget, that _could be_ explicitly specified by 
the user, but most of the times they may be inferred from the [widget types](#widget-types).

This is done to avoid updating `x` and `y` coordinates in the SQL files when you want to change the order of the tiles.

We recommend using `000_` prefix for the SQL files to keep the order of the tiles in the dashboard consistent, where the 
`000_` is the top of the dashboard and `999_` is the bottom. The first two digits would represent a row, and the last digit
is used to order the tiles within the row.

| Option                                     | Move widget effort | Mix `dashboard.yml` and `.sql` files |
|--------------------------------------------|--------------------|--------------------------------------|
| `x` and `y` coordinates                    | 🚨 high            | ✅ easy                              |
| `order` key in the SQL file                | ✅ low             | ✅ easy                              |
| `tiles` order in the  `dashboard.yml` file | ✅ low             | ⚠️ collisions possible               |
| filename prefix                            | ✅ low             | ⚠️ collisions possible               |

Order starts with `0` and in case of the `order` field conflict, we use the filename as a tie-breaker.

Let's take the following example:

* `query1.sql` has no explicit order
* `query2.sql` has no explicit order
* `query3.sql` has `order: 999`
* `query4.sql` has no explicit order
* `query5.sql` has `order: 2`

The order of the tiles from left-to-right and top-to-bottom in the dashboard would be as follows:

1. `query1.sql`
2. `query2.sql`
3. `query5.sql`
4. `query4.sql`
5. `query3.sql`

[[back to top](#dashboards-as-code)]

### Widget identifiers

By default, we'll use the filename as the widget identifier, but you can override it by specifying the `id` key in the
[SQL file metadata](#metadata).

[[back to top](#dashboards-as-code)]

### Database name replacement

You can define and test your SQL queries in a separate development database, the name of which is checked into 
the source control. We assume that the database name defined in the source control is a development reference database,
and it would most likely have a different name in the environment where the dashboard is deployed.

| Option                             | SQL copy-paste            | Valid Syntax    | Use as library  | Use for CI/CD    | Lib complexity |
|------------------------------------|---------------------------|-----------------|-----------------|------------------|----------------|
| Rewrite SQL AST                    | ✅                         | ✅               | ✅               | ✅                | 🚨 most        | 
| use a variable (e.g. `$inventory`) | 🚨 manual change required | ⚠️ syntax error | ✅               | ✅                | ⚠️ some        |
| do not replace database            | ✅                         | ✅               | 🚨 not reusable | ⚠️ no dev/prod   | ✅ none         | 
| use a separate branch              | ✅                         | ✅               | ✅               | ⚠️ complex setup | ✅ none         |

[[back to top](#dashboards-as-code)]

### Overrides

Overrides are used to augment the metadata that is defined in the SQL files with the lower-level Databricks Lakeview
entities.

[[back to top](#dashboards-as-code)]

## `.md` files

Markdown files are used to define text widgets that can populate a dashboard. 

The configuration is defined at the top of the file in YAML enclosed by two horizontal rules marked with dashes (---):

``` md
---
order: -1
height: 5
---
# Churn dashboard

Welcome to our churn dashboard! Let me show you around ...
```

### Text widget arguments

The following text widget arguments are supported:

| Flag          | Description                                 | Type       | Optional |
|---------------|---------------------------------------------|------------|----------|
| id            | The widget identifier                       | str        | Yes      |
| order         | The order of the widget                     | int        | Yes      |
| width         | The number of columns that the widget spans | int        | Yes      |
| height        | The number of rows that the widget spans    | int        | Yes      |
| title         | The widget title                            | str        | Yes      |
| description   | The widget description                      | str        | Yes      |

[[back to top](#dashboards-as-code)]

## `dashboard.yml` file

The `dashboard.yml` file is used to define a top-level metadata for the dashboard, such as the display name. Also,
this file may contain overrides for the [widgets](#widget-types), but we aim at mostly
[inferring it](#implicit-detection) from the SQL files. The file requires the `display_name` field, other fields are
optional. See below for the configuration schema:

```yml
display_name: <display name>

tiles:
  <tile id>:
    order: <order>
    width: <width>
    height: <height>
    title: <title>
    description: <description>
    type: <type>
    filter: 
      - <column>
      - <column>
  <tile id>:
    ...
  ...
```


[[back to top](#dashboards-as-code)]

## Using as library

It is possible to this project as a library, so that you can embed the generation of the dashboards in your own code.

[[back to top](#dashboards-as-code)]

## Configuration precedence

The precedence of the configuration is as follows:

* command-line flags
* [SQL file](#headers-of-sql-files) comment headers
* [`dashboard.yml`](#dashboardyml-file) file
* [SQL query content](#implicit-detection)

[[back to top](#dashboards-as-code)]

# Command-line interface

The command-line interface is used to generate the dashboard from the configuration files.

[[back to top](#dashboards-as-code)]