[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "task_table_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "tasks.id", DestinationColumnName = "tasks.id"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared task_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "task_table", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 3", {{"tasks.created_time", type datetime}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each [tasks.created_time] >= #datetime(2025, 1, 1, 0, 0, 0) and [tasks.created_time] <= #datetime(2025, 6, 30, 0, 0, 0)),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each [tasks.log_hours.total_hours] <> "00:00"),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows 1", {"tasks.id"})
in
  #"Removed other columns";
shared task_table_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  TableNavigation = Navigation_2{[Id = "filtered_task_id_table", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
