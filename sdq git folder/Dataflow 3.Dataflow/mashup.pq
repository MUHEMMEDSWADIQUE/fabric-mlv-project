[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Airlines_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Airlines = let
  Source = OData.Feed("https://services.odata.org/V4/TripPinServiceRW/", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Airlines", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared Airports = let
  Source = OData.Feed("https://services.odata.org/V4/TripPinServiceRW/", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Airports", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared GetNearestAirport = let
  Source = OData.Feed("https://services.odata.org/V4/TripPinServiceRW/", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "GetNearestAirport", Signature = "function (lat as number, lon as number) as record"]}[Data]
in
  #"Navigation 1";
shared Me = let
  Source = OData.Feed("https://services.odata.org/V4/TripPinServiceRW/", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Me", Signature = "singleton"]}[Data]
in
  #"Navigation 1";
shared People = let
  Source = OData.Feed("https://services.odata.org/V4/TripPinServiceRW/", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "People", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared Photos = let
  Source = OData.Feed("https://services.odata.org/V4/TripPinServiceRW/", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Photos", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared Airlines_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "66e95359-350d-4abd-9b10-110ec6cfd52b"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8dc76fcf-6ff3-413f-90a1-c1d732ffa899"]}[Data],
  Navigation_3 = Navigation_2{[Id = "test", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Airlines_Odata", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
