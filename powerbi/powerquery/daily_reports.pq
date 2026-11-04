// Query: daily_reports
// Reads the committed CSV; replaces automatic type inference with explicit types.
// pDataFolder is the shared Text parameter containing the powerbi/data folder path.
let
    RawCsv = Csv.Document(File.Contents(pDataFolder & "\daily_reports.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
    Headers = Table.PromoteHeaders(RawCsv, [PromoteAllScalars = true]),
    // en-US reads the dot decimal separator in the CSV consistently on any PC.
    TypedColumns = Table.TransformColumnTypes(Headers, {
        {"validation_run_id", type text},
        {"captured_at", type text},
        {"reporting_key", type text},
        {"batch_id", type text},
        {"payment_date", type date},
        {"source_system", type text},
        {"payment_row_count", Int64.Type},
        {"distinct_payment_count", Int64.Type},
        {"valid_payment_count", Int64.Type},
        {"flagged_payment_count", Int64.Type},
        {"payment_error_row_count", Int64.Type},
        {"duplicate_payment_id_count", Int64.Type},
        {"validation_pass_rate_pct", type number},
        {"valid_net_amount_eur", Currency.Type}
    }, "en-US"),
    // Keep UTC clock values before loading plain DateTime columns into the model.
    UtcTimes = Table.TransformColumns(TypedColumns, {
        {"captured_at", each DateTimeZone.RemoveZone(
            DateTimeZone.ToUtc(DateTimeZone.FromText(_))), type datetime}
    }),
    UtcColumnNames = Table.RenameColumns(UtcTimes, {
        {"captured_at", "captured_at_utc"}
    })
in
    UtcColumnNames
