[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared all_projects_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "all_projects_table", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared portal_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "portal_table", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared issues_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "issues_table", ItemKind = "Table"]}[Data],
  #"Counted rows" = Table.RowCount(#"Navigation 3")
in
  #"Counted rows";
shared phases_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "phases_table", ItemKind = "Table"]}[Data],
  #"Counted rows" = Table.RowCount(#"Navigation 3")
in
  #"Counted rows";
shared task_lists_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "task_lists_table", ItemKind = "Table"]}[Data],
  #"Counted rows" = Table.RowCount(#"Navigation 3")
in
  #"Counted rows";
shared task_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "task_table", ItemKind = "Table"]}[Data],
  #"Counted rows" = Table.RowCount(#"Navigation 3")
in
  #"Counted rows";
shared users_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "users_table", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 3", each [users.role] <> null and [users.role] <> ""),
  #"Counted rows" = Table.RowCount(#"Filtered rows")
in
  #"Counted rows";
shared leaves_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "leaves_table", ItemKind = "Table"]}[Data],
  #"Counted rows" = Table.RowCount(#"Navigation 3")
in
  #"Counted rows";
shared tasks_timelogs_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "tasks_timelogs_table", ItemKind = "Table"]}[Data],
  #"Counted rows" = Table.RowCount(#"Navigation 3")
in
  #"Counted rows";
shared Project_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Project_table", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared filtered_task_id_table = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "filtered_task_id_table", ItemKind = "Table"]}[Data],
  #"Counted rows" = Table.RowCount(#"Navigation 3")
in
  #"Counted rows";
shared #"task_table (2)" = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "task_table", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 3", {{"tasks.created_time", type datetime}}),
  #"Sorted rows" = Table.Sort(#"Changed column type", {{"tasks.created_time", Order.Descending}})
in
  #"Sorted rows";
shared #"tasks_timelogs_table (2)" = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7c8e3de7-f4b0-4df6-aef4-a0bd8c795c8b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "tasks_timelogs_table", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 3", {{"time_logs.date", type date}}),
  #"Sorted rows" = Table.Sort(#"Changed column type", {{"time_logs.date", Order.Descending}}),
  #"Filtered rows" = Table.SelectRows(#"Sorted rows", each ([time_logs.log_details.id] = "1918757000010868037"))
in
  #"Filtered rows";
