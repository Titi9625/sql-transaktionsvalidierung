// Query: findings
// Reads the committed CSV; replaces automatic type inference with explicit types.
// pDataFolder is the shared Text parameter containing the powerbi/data folder path.
let
    RawCsv = Csv.Document(File.Contents(pDataFolder & "\findings.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
    Headers = Table.PromoteHeaders(RawCsv, [PromoteAllScalars = true]),
    // Databricks exported missing bank-row references as the literal text null.
    MissingBankRows = Table.ReplaceValue(Headers, "null", null,
        Replacer.ReplaceValue, {"related_bank_row_number"}),
    // en-US reads the dot decimal separator in the CSV consistently on any PC.
    TypedColumns = Table.TransformColumnTypes(MissingBankRows, {
        {"validation_run_id", type text},
        {"captured_at", type text},
        {"batch_id", type text},
        {"source_row_number", Int64.Type},
        {"transaction_id", type text},
        {"record_source", type text},
        {"error_code", type text},
        {"error_description", type text},
        {"error_field", type text},
        {"related_bank_row_number", Int64.Type},
        {"detected_at", type text}
    }, "en-US"),
    // Keep UTC clock values before loading plain DateTime columns into the model.
    UtcTimes = Table.TransformColumns(TypedColumns, {
        {"captured_at", each DateTimeZone.RemoveZone(
            DateTimeZone.ToUtc(DateTimeZone.FromText(_))), type datetime},
        {"detected_at", each DateTimeZone.RemoveZone(
            DateTimeZone.ToUtc(DateTimeZone.FromText(_))), type datetime}
    }),
    UtcColumnNames = Table.RenameColumns(UtcTimes, {
        {"captured_at", "captured_at_utc"},
        {"detected_at", "detected_at_utc"}
    })
in
    UtcColumnNames
