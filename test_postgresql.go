
// Author: Cayetano Benavent, 2015.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
// MA 02110-1301, USA.


package main

import (
    "fmt"
    
    "database/sql"

    _ "github.com/lib/pq"
    
    "encoding/json"
)


type myJSON struct {
    Munic string
    Pob int
    Cod_ine int
 }
 
type DBdataconfig struct {
    DBdriver string 
    User string
    Pswd string
    DBname string
    Port int
 }
 

func padronAndalucia(dbcfg DBdataconfig, slct_query string, pobval int) {
    
    conn_data := fmt.Sprintf("user=%s password=%s dbname=%s port=%v", 
                             dbcfg.User, dbcfg.Pswd,dbcfg.DBname, dbcfg.Port)
    
    db, err := sql.Open(dbcfg.DBdriver, conn_data)
    
    fmt.Printf("\n---Successful connection to: %s\n\n", dbcfg.DBname)
    
    if err != nil {
        fmt.Printf("\nOpen error: %v", err)
    }
    
    pob := pobval
    rows, err := db.Query(slct_query, pob)
    
    if err != nil {
        fmt.Printf("\nQuery error: %v\n", err)
        
    } else {
        
        defer rows.Close()
        
        var jsonSlice []string

        for rows.Next() {
            var cod_ine int
            var municipio string
            var poblacion int
            
            if err := rows.Scan(&cod_ine, &municipio, &poblacion); err != nil {
                fmt.Printf("Query error: %v\n", err)
                
            }
            
            myjson := &myJSON{municipio, poblacion, cod_ine}
            
            jsn, err := json.Marshal(myjson)
            if err != nil {
                fmt.Printf("Error: %s", err)
            
            }
            
            jsonSlice = append(jsonSlice, string(jsn))
            
            fmt.Printf("\t%v - %s - %v\n", cod_ine, municipio, poblacion)
        
        }
        
        fmt.Printf("\n---Successful query!\n")
        
        jsn_complete, err := json.Marshal(jsonSlice)
        if err != nil {
                fmt.Printf("Error: %s", err)
            }
        
        fmt.Println(string(jsn_complete))

    }

}

func main() {
    
    dbcfg := DBdataconfig{DBdriver: "postgres", 
                            User: "postgres", 
                            Pswd: "postgres", 
                            DBname: "my_postgis_db",
                            Port: 5432}
    
    slct_query := `SELECT cod_ine, municipio, poblacion 
                    FROM public.padron2012_andalucia 
                    WHERE poblacion > $1 
                    ORDER BY poblacion DESC`
                    
    pobval := 50000
    
    padronAndalucia(dbcfg, slct_query, pobval)

}
