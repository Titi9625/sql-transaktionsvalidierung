// Query: report_runs
// Reads the committed CSV; replaces automatic type inference with explicit types.
// pDataFolder is the shared Text parameter containing the powerbi/data folder path.
let
    RawCsv = Csv.Document(File.Contents(pDataFolder & "\report_runs.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
    Headers = Table.PromoteHeaders(RawCsv, [PromoteAllScalars = true]),
    // en-US reads the dot decimal separator in the CSV consistently on any PC.
    TypedColumns = Table.TransformColumnTypes(Headers, {
        {"validation_run_id", type text},
        {"captured_at", type text},
        {"definition_fingerprint", type text},
        {"batch_id", type text},
        {"validation_date", type date},
        {"total_payment_records", Int64.Type},
        {"total_bank_records", Int64.Type},
        {"distinct_payment_count", Int64.Type},
        {"valid_records", Int64.Type},
        {"valid_payment_count", Int64.Type},
        {"invalid_payment_transactions", Int64.Type},
        {"unclassified_payment_count", Int64.Type},
        {"total_validation_errors", Int64.Type},
        {"payment_error_rows", Int64.Type},
        {"bank_error_rows", Int64.Type},
        {"duplicate_errors", Int64.Type},
        {"missing_value_errors", Int64.Type},
        {"currency_errors", Int64.Type},
        {"pending_errors", Int64.Type},
        {"refund_errors", Int64.Type},
        {"amount_mismatch_errors", Int64.Type},
        {"missing_bank_match_errors", Int64.Type},
        {"unmatched_bank_transactions", Int64.Type},
        {"bank_reference_id_not_extractable_errors", Int64.Type},
        {"validation_pass_rate_pct", type number}
    }, "en-US"),
    // Keep UTC clock values before loading plain DateTime columns into the model.
    UtcTimes = Table.TransformColumns(TypedColumns, {
        {"captured_at", each DateTimeZone.RemoveZone(
            DateTimeZone.ToUtc(DateTimeZone.FromText(_))), type datetime}
    }),
    UtcColumnNames = Table.RenameColumns(UtcTimes, {
        {"captured_at", "captured_at_utc"}
    }),
    // Scenario sequence is separate from the dates when reports were captured.
    NamedDelivery = Table.AddColumn(UtcColumnNames, "delivery_name", each
        if [batch_id] = "8199572c3db5105dd6a290e6fc969eea50df9f896301a23881f6febbfe98936d" then "01 Initial delivery"
        else if [batch_id] = "928cceb410a894c9ee04ac9d58c33100e4997d9a23d84b6125d73a5b81f3a27d" then "02 Updated delivery"
        else "Other delivery", type text),
    DeliveryOrder = Table.AddColumn(NamedDelivery, "delivery_order", each
        if [batch_id] = "8199572c3db5105dd6a290e6fc969eea50df9f896301a23881f6febbfe98936d" then 1
        else if [batch_id] = "928cceb410a894c9ee04ac9d58c33100e4997d9a23d84b6125d73a5b81f3a27d" then 2
        else 99, Int64.Type),
    DisplayColumns = Table.ReorderColumns(DeliveryOrder,
        {"delivery_name", "delivery_order"} & Table.ColumnNames(UtcColumnNames)),
    SortedDelivery = Table.Sort(DisplayColumns, {{"delivery_order", Order.Ascending}})
in
    SortedDelivery
