[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "MP leads downloads to DB_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Engagement ID", DestinationColumnName = "Engagement ID"], [SourceColumnName = "Referral ID", DestinationColumnName = "Referral ID"], [SourceColumnName = "Lead Name", DestinationColumnName = "Lead Name"], [SourceColumnName = "Customer Name", DestinationColumnName = "Customer Name"], [SourceColumnName = "Customer Address Line 1", DestinationColumnName = "Customer Address Line 1"], [SourceColumnName = "Customer Address Line 2", DestinationColumnName = "Customer Address Line 2"], [SourceColumnName = "Customer City", DestinationColumnName = "Customer City"], [SourceColumnName = "Customer State", DestinationColumnName = "Customer State"], [SourceColumnName = "Customer Postal Code", DestinationColumnName = "Customer Postal Code"], [SourceColumnName = "Customer Country", DestinationColumnName = "Customer Country"], [SourceColumnName = "Customer D-U-N-S id", DestinationColumnName = "Customer D-U-N-S id"], [SourceColumnName = "Customer Contact First Name", DestinationColumnName = "Customer Contact First Name"], [SourceColumnName = "Customer Contact Last Name", DestinationColumnName = "Customer Contact Last Name"], [SourceColumnName = "Customer Contact Phone number", DestinationColumnName = "Customer Contact Phone number"], [SourceColumnName = "Phone Validity", DestinationColumnName = "Phone Validity"], [SourceColumnName = "Customer Contact Email Address", DestinationColumnName = "Customer Contact Email Address"], [SourceColumnName = "Email Validity", DestinationColumnName = "Email Validity"], [SourceColumnName = "Partner Referral Status", DestinationColumnName = "Partner Referral Status"], [SourceColumnName = "Partner Referral Substatus", DestinationColumnName = "Partner Referral Substatus"], [SourceColumnName = "Decline/Lost/Error reason", DestinationColumnName = "Decline/Lost/Error reason"], [SourceColumnName = "Sales Stage", DestinationColumnName = "Sales Stage"], [SourceColumnName = "Estimated Deal Value", DestinationColumnName = "Estimated Deal Value"], [SourceColumnName = "Currency", DestinationColumnName = "Currency"], [SourceColumnName = "Referral Program", DestinationColumnName = "Referral Program"], [SourceColumnName = "Lead source", DestinationColumnName = "Lead source"], [SourceColumnName = "Call to action", DestinationColumnName = "Call to action"], [SourceColumnName = "Lead quality", DestinationColumnName = "Lead quality"], [SourceColumnName = "Lead type", DestinationColumnName = "Lead type"], [SourceColumnName = "CRM ID", DestinationColumnName = "CRM ID"], [SourceColumnName = "Marketing Campaign ID", DestinationColumnName = "Marketing Campaign ID"], [SourceColumnName = "Notes", DestinationColumnName = "Notes"], [SourceColumnName = "MPN Id", DestinationColumnName = "MPN Id"], [SourceColumnName = "Lead Created Date", DestinationColumnName = "Lead Created Date"], [SourceColumnName = "Offer 1", DestinationColumnName = "Offer 1"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"MP leads downloads to DB" = let
  Source = SharePoint.Files(#"Sharepoint site", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each [Folder Path] = #"Data folder"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each Text.StartsWith([Name], Filename) and [Extension] = ".csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file", each #"Transform file"([Content])),
  #"Renamed columns" = Table.RenameColumns(#"Invoke custom function", {{"Name", "Source.Name"}}),
  #"Removed other columns" = Table.SelectColumns(#"Renamed columns", {"Source.Name", "Transform file"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))),
  #"Filtered rows 2" = Table.SelectRows(#"Expanded table column", each [Source.Name] <> "ExportLeads_marketplace_all.csv"),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows 2", {{"Engagement ID", type text}, {"Referral ID", type text}, {"Lead Name", type text}, {"Customer Name", type text}, {"Customer Address Line 1", type text}, {"Customer Address Line 2", type text}, {"Customer City", type text}, {"Customer State", type text}, {"Customer Postal Code", type text}, {"Customer Country", type text}, {"Customer D-U-N-S id", type text}, {"Customer Contact First Name", type text}, {"Customer Contact Last Name", type text}, {"Customer Contact Phone number", type text}, {"Phone Validity", type text}, {"Customer Contact Email Address", type text}, {"Email Validity", type text}, {"Partner Referral Status", type text}, {"Partner Referral Substatus", type text}, {"Decline/Lost/Error reason", type text}, {"Sales Stage", Int64.Type}, {"Estimated Deal Value", type text}, {"Currency", type text}, {"Estimated Close Date", type date}, {"Referral Program", type text}, {"Lead source", type text}, {"Call to action", type text}, {"Lead quality", type text}, {"Lead type", type text}, {"CRM ID", type text}, {"Marketing Campaign ID", type text}, {"Notes", type text}, {"MPN Id", Int64.Type}, {"Lead Created Date", type date}, {"Offer 1", type text}, {"Errors", type text}}),
  #"Choose columns" = Table.SelectColumns(#"Changed column type", {"Engagement ID", "Referral ID", "Lead Name", "Customer Name", "Customer Address Line 1", "Customer Address Line 2", "Customer City", "Customer State", "Customer Postal Code", "Customer Country", "Customer D-U-N-S id", "Customer Contact First Name", "Customer Contact Last Name", "Customer Contact Phone number", "Phone Validity", "Customer Contact Email Address", "Email Validity", "Partner Referral Status", "Partner Referral Substatus", "Decline/Lost/Error reason", "Sales Stage", "Estimated Deal Value", "Currency", "Referral Program", "Lead source", "Call to action", "Lead quality", "Lead type", "CRM ID", "Marketing Campaign ID", "Notes", "MPN Id", "Lead Created Date", "Offer 1"}),
  #"Removed duplicates" = Table.Distinct(#"Choose columns")
in
  #"Removed duplicates";
shared #"Sharepoint site" = "https://yavdain.sharepoint.com/sites/PBIVizEdit-Marketing" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared #"Data folder" = "https://yavdain.sharepoint.com/sites/PBIVizEdit-Marketing/Shared Documents/Marketing/Reports/Data/Referral Leads Raw Downloads/" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared Filename = "ExportLeads_" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared #"Sample file" = let
  Source = SharePoint.Files(#"Sharepoint site", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each [Folder Path] = #"Data folder"),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each Text.StartsWith([Name], Filename) and [Extension] = ".csv"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows 1", each [Attributes]?[Hidden]? <> true),
  Navigation = #"Filtered hidden files"{0}[Content]
in
  Navigation;
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Csv.Document(Parameter, [Delimiter = ",",  Encoding = 65001]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Csv.Document(Parameter, [Delimiter = ",",  Encoding = 65001]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
shared #"MP leads downloads to DB_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "4eb34a24-0d28-44c2-b26c-d20ec4c41693"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "f5fc4bfa-cbc1-4652-889b-820bba9432f7"]}[Data],
  TableNavigation = Navigation_2{[Item = "MP_leads", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
