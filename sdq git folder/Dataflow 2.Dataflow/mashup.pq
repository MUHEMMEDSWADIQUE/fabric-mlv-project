[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"countries csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "66e95359-350d-4abd-9b10-110ec6cfd52b"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "8dc76fcf-6ff3-413f-90a1-c1d732ffa899"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "test_files"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "countries.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 7, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"order_id", Int64.Type}, {"customer_id", type text}, {"order_date", type datetime}, {"product_id", type text}, {"quantity", Int64.Type}, {"price", Int64.Type}, {"status", type text}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "new_col", each [customer_id]), {{"new_col", type text}}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom", "processed time", each DateTime.LocalNow()), {{"processed time", type datetime}})
in
  #"Added custom 1";
shared #"payment csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "66e95359-350d-4abd-9b10-110ec6cfd52b"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "8dc76fcf-6ff3-413f-90a1-c1d732ffa899"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "test_files"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "payment.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 5, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"order_id", Int64.Type}, {"payment_id", type text}, {"payment_date", type date}, {"payment_method", type text}, {"payment_status", type text}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Merge_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Merge = let
  Source = Table.NestedJoin(#"countries csv", {"order_id"}, #"payment csv", {"order_id"}, "payment csv", JoinKind.LeftOuter)
in
  Source;
shared Merge_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "66e95359-350d-4abd-9b10-110ec6cfd52b"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8dc76fcf-6ff3-413f-90a1-c1d732ffa899"]}[Data],
  Navigation_3 = Navigation_2{[Id = "test", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "new_merged_tables", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
