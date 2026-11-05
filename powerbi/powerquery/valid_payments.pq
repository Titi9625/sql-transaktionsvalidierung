// Query: valid_payments
// Reads the committed CSV; replaces automatic type inference with explicit types.
// pDataFolder is the shared Text parameter containing the powerbi/data folder path.
let
    RawCsv = Csv.Document(File.Contents(pDataFolder & "\valid_payments.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
    Headers = Table.PromoteHeaders(RawCsv, [PromoteAllScalars = true]),
    // en-US reads the dot decimal separator in the CSV consistently on any PC.
    TypedColumns = Table.TransformColumnTypes(Headers, {
        {"validation_run_id", type text},
        {"captured_at", type text},
        {"batch_id", type text},
        {"source_row_number", Int64.Type},
        {"bank_source_row_number", Int64.Type},
        {"bank_transaction_id", type text},
        {"transaction_id", type text},
        {"transaction_date", type date},
        {"amount_gross", Currency.Type},
        {"fee_amount", Currency.Type},
        {"amount_net", Currency.Type},
        {"bank_amount", Currency.Type},
        {"currency", type text},
        {"status", type text},
        {"validation_status", type text}
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
