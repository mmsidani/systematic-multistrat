﻿#r@"..\sa2-dlls\common.dll"


open SA2.Common.Dictionaries


let icb2GicsConverter =

    {

        // Format : ( gics number , list of icb numbers that should map to that gics number )

        // Note: in the current implementation below, ICB subsector numbers were paired to Gics level 3
        
        gicsIcb = 

        [

            ( "101010" , [ "573" ] ) ;
            ( "101020" , [ "533" ; "537" ; "577" ] ) ;
            ( "151010" , [ "1353" ; "1357" ] ) ;
            ( "151020" , [ "2353" ] ) ;
            ( "151030" , [ "2723" ] ) ;
            ( "151040" , [ "1753" ; "1755" ; "1757" ; "1771" ; "1773" ; "1775" ; "1777" ; "1779" ] ) ;
            ( "151050" , [ "1733" ; "1737" ] ) ;
            ( "201010" , [ "2713" ; "2717" ] ) ;
            ( "201020" , [ ] ) ;
            ( "201030" , [ "2357" ] ) ;
            ( "201040" , [ "2733" ; "2737" ] ) ;
            ( "201050" , [ "2727" ] ) ;
            ( "201060" , [ "2753" ; "2757" ] ) ;
            ( "201070" , [ ] ) ;
            ( "202010" , [ "2791" ; "2797" ; "2799" ] ) ;
            ( "202020" , [ "2793" ; "2795" ] ) ;
            ( "203010" , [ "2771" ] ) ;
            ( "203020" , [ "5751" ] ) ;
            ( "203030" , [ "2773" ] ) ;
            ( "203040" , [ "2775" ; "2779" ] ) ;
            ( "203050" , [ "2777" ] ) ;
            ( "251010" , [ "3355" ; "3357" ] ) ;
            ( "251020" , [ "3353" ] ) ;
            ( "252010" , [ "3722" ; "3726" ; "3728" ; "3743" ] ) ;
            ( "252020" , [ "3745" ; "3747" ] ) ;
            ( "252030" , [ "3763" ; "3765" ] ) ;
            ( "253010" , [ "5752" ; "5753" ; "5755" ; "5757" ; "5759" ] ) ;
            ( "253020" , [ "5377" ] ) ;
            ( "254010" , [ "5553" ; "5555" ; "5557" ] ) ;
            ( "255010" , [ ] ) ;
            ( "255020" , [ ] ) ;
            ( "255030" , [ "5373" ] ) ;
            ( "255040" , [ "5371" ; "5375" ; "5379" ] ) ;
            ( "301010" , [ "5333" ; "5337" ] ) ;
            ( "302010" , [ "3533" ; "3535" ; "3537" ] ) ;
            ( "302020" , [ "3573" ; "3577" ] ) ;
            ( "302030" , [ "3785" ] ) ;
            ( "303010" , [ "3724" ] ) ;
            ( "303020" , [ "3767" ] ) ;
            ( "351010" , [ "4535" ; "4537" ] ) ;
            ( "351020" , [ "4533" ] ) ;
            ( "351030" , [ ] ) ;
            ( "352010" , [ "4573" ] ) ;
            ( "352020" , [ "4577" ] ) ;
            ( "352030" , [ ] ) ;
            ( "401010" , [ "8355" ; "8779" ] ) ;
            ( "401020" , [ ] ) ;
            ( "402010" , [ "8775" ] ) ;
            ( "402020" , [ "8773" ] ) ;
            ( "402030" , [ "8771" ; "8777" ; "8985" ; "8995" ] ) ;
            ( "403010" , [ "8532" ; "8534" ; "8536" ; "8538" ; "8575" ] ) ;
            ( "404020" , [ "8633" ; "8671" ; "8672" ; "8673" ; "8674" ; "8675" ; "8676" ; "8677" ] ) ;
            ( "404030" , [ "8637" ] ) ;
            ( "451010" , [ "9535" ] ) ;
            ( "451020" , [ ] ) ;
            ( "451030" , [ "9537" ] ) ;
            ( "452010" , [ "9578" ] ) ;
            ( "452020" , [ "9533" ; "9572" ] ) ;
            ( "452030" , [ ] ) ;
            ( "452040" , [ "9574" ] ) ;
            ( "453010" , [ "9576" ] ) ;
            ( "501010" , [ "6535" ] ) ;
            ( "501020" , [ "6575" ] ) ;
            ( "551010" , [ "7535" ; "7537" ] ) ;
            ( "551020" , [ "7573" ] ) ;
            ( "551030" , [ "7575" ] ) ;
            ( "551040" , [ "7577" ] ) ;
            ( "551050" , [ "583" ; "587" ] )

        ]

    }
