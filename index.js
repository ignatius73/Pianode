
require('dotenv').config();
const csvdata = require("csvdata");
const datos = require('./simulated_data.json');
const fetch = require('node-fetch');
const files = ['filea.csv', 'fileb.csv'];

const apidata = { "aid": process.env.aid, "api_token": process.env.api_token};

//Endpoint was infered from the available documentation because no one endpoint I checked responded.
const url = 'sandbox.tinypass.com/publisher/users/get';
const fieldnames = ['user_id', 'email', 'first_name', 'last_name'];

function MergedCSV(f){
    return new Promise( resolve => {
        
        let data = [];
        const fieldnames = ['user_id', 'email', 'first_name', 'last_name'];
    
        csvdata.load(`./input/${f}`)
            .then( resp =>{
                for ( let r of resp ){
    
                let obj = r
                
                for( let f of fieldnames ){
                   if (!isKeyExists(obj, f)){
                        obj[f] = '';
                   }
                }
    
                data.push(obj); 
                
                }
                resolve(data);
            });
          

        });
        
    }



async function MergeCSV(){

    const merged = [];
    
    for (let f of files){
        let data = await MergedCSV(f);
        for (let obj of data){
            merged.push(obj);
        }
           
    }
    
    FixUserID(merged); 
    
}

//Fix the users_id 
function FixUserID(merged){

    for ( let m of merged ){
        if (m.email === ''){
            let found = datos.data.find( e=> 
                e.first_name ===  m.first_name && e.last_name === m.last_name && e.user_id !== m.user_id);
                
            
            if( found !== undefined ){
               m.user_id = found.user_id; 
            
            }
            
            
        }else{
            let found = datos.data.find( e=> 
            e.email ===  m.email && e.user_id !== m.user_id);
                        
            if( found !== undefined ){
                m.user_id = found.user_id; 
            
            }

        }
 
     }
        
    // writes the file merged
    csvdata.write('./output/merged.csv', merged, {append: false, header:'user_id,email,first_name,last_name', emptyValues: true})
        .then( resp => {
            console.log("The files were merged successfully");
        })
        .catch( e =>{
            throw new Error(e);
        });
    

}

//Example of a fetch data from one endpoint
fetch(url, 
    {
        method:'POST',
        body: JSON.stringify(apidata),
        headers: { 'Content-Type': 'application/json'} 
    }).then( resp =>{
         resp.json().then(json => {
             data = json;
             MergeCSV();
            });
         


    }).catch( err => {
        console.log("Connection Error");
    });

 
//Function to check if the keys in the csv structure exists
function isKeyExists(obj,key){
    return key in obj;
}