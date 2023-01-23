#r@"..\sa2-dlls\common.dll"


open SA2.Common.RatingsMapTypes



let ratingsMap =

    {

        moodys = 

            [

            ( "Aaa" , 1 ) ;
            ( "Aa1" , 2 ) ;
            ( "Aa2" , 3 ) ;
            ( "Aa3" , 4 ) ;
            ( "A1" , 5 ) ;
            ( "A2" , 6 ) ;
            ( "A3" , 7 ) ;
            ( "Baa1" , 8 ) ;
            ( "Baa2" , 9 ) ;
            ( "Baa3" , 10 ) ;
            ( "Ba1" , 11 ) ;
            ( "Ba2" , 12 ) ;
            ( "Ba3" , 13 ) ;
            ( "B1" , 14 ) ;
            ( "B2" , 15 ) ;
            ( "B3" , 16 ) ;
            ( "Caa1" , 17 ) ;
            ( "Caa2" , 18 ) ;
            ( "Caa3" , 19 ) 

            ] ;

        snp =

            [

            ( "AAA" , 1 ) ;
            ( "AA+" , 2 ) ;
            ( "AA" , 3 ) ;
            ( "AA-" , 4 ) ;
            ( "A+" , 5 ) ;
            ( "A" , 6 ) ;
            ( "A-" , 7 ) ;
            ( "BBB+" , 8 ) ;
            ( "BBB" , 9 ) ;
            ( "BBB-" , 10 ) ;
            ( "BB+" , 11 ) ;
            ( "BB" , 12 ) ;
            ( "BB-" , 13 ) ;
            ( "B+" , 14 ) ;
            ( "B" , 15 ) ;
            ( "B-" , 16 ) ;
            ( "CCC+" , 17 ) ;
            ( "CCC" , 18 ) ;
            ( "CCC-" , 19 ) ;

            ] ;

        fitch =

            [

            ( "AAA" , 1 ) ;
            ( "AA+" , 2 ) ;
            ( "AA" , 3 ) ;
            ( "AA-" , 4 ) ;
            ( "A+" , 5 ) ;
            ( "A" , 6 ) ;
            ( "A-" , 7 ) ;
            ( "BBB+" , 8 ) ;
            ( "BBB" , 9 ) ;
            ( "BBB-" , 10 ) ;
            ( "BB+" , 11 ) ;
            ( "BB" , 12 ) ;
            ( "BB-" , 13 ) ;
            ( "B+" , 14 ) ;
            ( "B" , 15 ) ;
            ( "B-" , 16 ) ;
            ( "CCC+" , 17 ) ; // bogus rating -- added to match moodys and sp
            ( "CCC" , 18 ) ;
            ( "CCC-" , 19 ) ; // bogus rating -- added to match moodys and sp

            ] 

    }