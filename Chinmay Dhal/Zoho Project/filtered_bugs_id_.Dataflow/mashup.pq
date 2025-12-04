[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "issues_table_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared issues_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "issues_table", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 3", {{"issues.created_time", type datetime}}),
  #"Sorted rows" = Table.Sort(#"Changed column type", {{"issues.created_time", Order.Ascending}}),
  #"Filtered rows" = Table.SelectRows(#"Sorted rows", each [issues.created_time] >= #datetime(2025, 10, 1, 0, 0, 0) and [issues.created_time] <= #datetime(2025, 11, 25, 0, 0, 0)),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows", {"issues.id"})
in
  #"Removed other columns";
shared issues_table_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  TableNavigation = Navigation_2{[Id = "filtered_bugs_ids", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
