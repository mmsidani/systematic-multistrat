#r@"..\sa2-dlls\common.dll"


open SA2.Common.Table_sa2_macro_data
open SA2.Common.Utils



let getMacroData names =

    let data = get_sa2_macro_data names

    data |> Map.iter ( fun i l -> printf "%s\n%s" i ( paste l ) )


// execute

getMacroData [ "us.gdprate" ; "uk.gdprate" ; "pl.gdprate" ]
