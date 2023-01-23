module SA2.Common.FxTypes


type ForexRateField = string

type ForexIndicator = string





type YieldConversionRates = {

    conversionRate : ( string * string ) list

}




type ForexDataFields = {

    forexRateField : ForexRateField

    forexIndicator : ForexIndicator

}
