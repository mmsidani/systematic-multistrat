#r@"..\sa2-dlls\common.dll"


#load ".\settingsProduction.fsx"


open SA2.Common.Table_instruments


let newInstruments = 

    [

        // format is : ( InstrumentTicker , Description )

        // NOTE: when copying and pasting remember to replace \r with \n

        


    ]

    |> Map.ofList


// execute. prints to the screen

update_instruments newInstruments
