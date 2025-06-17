# Shared Materializations

The materializations in this folder are copied from (here)[https://github.com/databricks/dbt-databricks/tree/main/dbt/include/databricks/macros/materializations]. If dbt-databricks is ever updated and these materializations break then this is a good place to look first for any updates.

The only difference between the base materializations and these ones is the extra step for sharing the model created with a delta share specified in the config of the model. N.B. Views have a further extra step to remove the share as the view cannot be dropped and recreated whilst shared. 