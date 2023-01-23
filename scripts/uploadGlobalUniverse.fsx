#r @"..\sa2-dlls\common.dll"



#load ".\settingsProduction.fsx"


open System
open SA2.Common.Table_instruments


let private globalDataInput =

        [

        ( "251010" , "MXWD0AP INDEX" ) ;
        ( "251020" , "MXWD0AU INDEX" ) ;
        ( "252010" , "MXWD0HD INDEX" ) ;
        ( "252020" , "MXWD0LE INDEX" ) ;
        ( "252030" , "MXWD0TA INDEX" ) ;
        ( "253020" , "MXWD0DC INDEX" ) ;
        ( "253010" , "MXWD0HL INDEX" ) ;
        ( "254010" , "MXWD0ME INDEX" ) ;
        ( "255010" , "MXWD0DT INDEX" ) ;
        ( "255020" , "MXWD0NC INDEX" ) ;
        ( "255030" , "MXWD0MR INDEX" ) ;
        ( "255040" , "MXWD0SR INDEX" ) ;
        ( "301010" , "MXWD0FS INDEX" ) ;
        ( "302010" , "MXWD0BV INDEX" ) ;
        ( "302020" , "MXWD0FP INDEX" ) ;
        ( "302030" , "MXWD0TB INDEX" ) ;
        ( "303010" , "MXWD0HH INDEX" ) ;
        ( "303020" , "MXWD0PP INDEX" ) ;
        ( "101010" , "MXWD0EE INDEX" ) ;
        ( "101020" , "MXWD0OG INDEX" ) ;
        ( "401010" , "MXWD0CB INDEX" ) ;
        ( "401020" , "MXWD0TM INDEX" ) ;
        ( "402030" , "MXWD0CK INDEX" ) ;
        ( "402020" , "MXWD0CF INDEX" ) ;
        ( "402010" , "MXWD0DS INDEX" ) ;
        ( "403010" , "MXWD0IR INDEX" ) ;
        ( "404020" , "MXWD0RI INDEX" ) ;
        ( "404030" , "MXWD0RM INDEX" ) ;
        ( "351010" , "MXWD0HE INDEX" ) ;
        ( "351020" , "MXWD0HV INDEX" ) ;
        ( "351030" , "MXWD0HT INDEX" ) ;
        ( "352010" , "MXWD0BT INDEX" ) ;
        ( "352030" , "MXWD0LS INDEX" ) ;
        ( "352020" , "MXWD0PH INDEX" ) ;
        ( "201010" , "MXWD0AD INDEX" ) ;
        ( "201020" , "MXWD0BP INDEX" ) ;
        ( "201030" , "MXWD0CE INDEX" ) ;
        ( "201040" , "MXWD0EL INDEX" ) ;
        ( "201050" , "MXWD0IC INDEX" ) ;
        ( "201060" , "MXWD0MA INDEX" ) ;
        ( "201070" , "MXWD0TD INDEX" ) ;
        ( "202010" , "MXWD0CO INDEX" ) ;
        ( "202020" , "MXWD0PS INDEX" ) ;
        ( "203010" , "MXWD0AF INDEX" ) ;
        ( "203020" , "MXWD0AL INDEX" ) ;
        ( "203030" , "MXWD0MN INDEX" ) ;
        ( "203040" , "MXWD0RR INDEX" ) ;
        ( "203050" , "MXWD0TI INDEX" ) ;
        ( "453010" , "MXWD0ST INDEX" ) ;
        ( "451020" , "MXWD0IF INDEX" ) ;
        ( "451010" , "MXWD0WS INDEX" ) ;
        ( "451030" , "MXWD0SO INDEX" ) ;
        ( "452010" , "MXWD0CQ INDEX" ) ;
        ( "452020" , "MXWD0PC INDEX" ) ;
        ( "452030" , "MXWD0EI INDEX" ) ;
        ( "452040" , "MXWD0OE INDEX" ) ;
        ( "151010" , "MXWD0CH INDEX" ) ;
        ( "151020" , "MXWD0CJ INDEX" ) ;
        ( "151030" , "MXWD0CP INDEX" ) ;
        ( "151040" , "MXWD0MM INDEX" ) ;
        ( "151050" , "MXWD0PF INDEX" ) ;
        ( "501010" , "MXWD0DV INDEX" ) ;
        ( "501020" , "MXWD0WT INDEX" ) ;
        ( "551010" , "MXWD0EU INDEX" ) ;
        ( "551020" , "MXWD0GU INDEX" ) ;
        ( "551050" , "MXWD0IP INDEX" ) ;
        ( "551030" , "MXWD0MU INDEX" ) ;
        ( "551040" , "MXWD0WU INDEX" )

        ] 

        |> Map.ofList

try

    // use the gics number as the 'Description' field

    update_instruments globalDataInput 

        ||> List.iter2 ( fun n i -> printfn "%s was assigned %d" n i )

with

    | e -> e.ToString() |> printfn "%s"