#r @"..\sa2-dlls\common.dll"



#load ".\settingsProduction.fsx"


open System

open SA2.Common.Table_instruments


#load ".\settingsFxConversion.fsx"



let conversionRateToCurrencyNameMap =

    SettingsFxConversion.fxConversionRates.conversionRate
    
        |> List.unzip

        |> ( fun ( l , r ) -> ( r , l |> List.map ( fun c -> c + " 3M Rate " ) ) )

        ||> List.zip
        
        |> Map.ofList

try

    // use the gics number as the 'Description' field

    update_instruments conversionRateToCurrencyNameMap 

        ||> List.iter2 ( fun n i -> printfn "%s was assigned %d" n i )

with

    | e -> e.ToString() |> printfn "%s"