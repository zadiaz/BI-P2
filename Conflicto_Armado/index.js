var fs = require('fs');


const procesar = async() => {
  
  let count_recursos = 0
  let count_homicidios = 0
  
  const _municipios = []
  const _fechas = []
  const _recursos = []
  
  
  const municipios = []
  const fechas = []
  const recursos = []
  const actividades_mineras = []
  const homicidios = []
  
  await fs.readFile('mineriaInfo.json', 'utf8', function(err, data) {
    if (err) {
      return console.log(err);
    }
    
    let data_actividades = JSON.parse(data)
    let count_actividades = 0
    data_actividades.forEach(_d =>{
      count_actividades++
      
      let data = {}
      
      data.municipio_id = parseInt(_d.codigo_dane)
      if(!_municipios[_d.codigo_dane]){
        _municipios[_d.codigo_dane] = {id:parseInt(_d.codigo_dane), municipio: _d.municipio_productor, departamento: _d.departamento  }
        municipios.push(_municipios[_d.codigo_dane])
      }
      
      data.fecha_id = parseInt(_d.a_o_produccion)
      if(!_fechas[_d.a_o_produccion]){
        _fechas[_d.a_o_produccion] = {id: parseInt(_d.a_o_produccion), anio: parseInt(_d.a_o_produccion) }
        fechas.push(_fechas[_d.a_o_produccion])
      }
      
      if(!_recursos[_d.recurso_natural]){
        count_recursos ++
        _recursos[_d.recurso_natural] = {id: count_recursos, tipo: _d.recurso_natural}
        recursos.push(_recursos[_d.recurso_natural])
      }
      
      data.unidad = _d.unidad_medida
      data.cantidad = _d.cantidad_producci_n
      data.recurso_id = _recursos[_d.recurso_natural]?_recursos[_d.recurso_natural].id : null
      data.id = count_actividades
      if(data.cantidad > 0){

        actividades_mineras.push(data)
      }
      
    })
    
    fs.writeFileSync('dimension_actividades_mineras.json',JSON.stringify(actividades_mineras,null,2),'utf-8');
    
    
    
  });
  
  await fs.readFile('CA.csv',  'utf8',function (err, fileData) {
    if (err) {
      return console.log(err);
    }
    const rows = fileData.split(/\r\n|\r|\n/, -1)
    
    rows.forEach(_r=>{
      if(count_homicidios > 0){
        let data = {cantidad: 0, desplazamientos: 0}
        let columns = _r.split(';')
        
        data.municipio_id = parseInt(columns[2])
        if(!_municipios[columns[2]]){
          _municipios[columns[2]] = {id:parseInt(columns[2]), municipio: columns[3], departamento: columns[1] }
          municipios.push(_municipios[columns[2]])
        }
        
        data.fecha_id = parseInt(columns[9])
        if(!_fechas[columns[9]]){
          _fechas[columns[9]] = {id: parseInt(columns[9]), anio: parseInt(columns[9]) }
          fechas.push(_fechas[columns[9]])
        }
        let add = false
        if(columns[6].includes('Homicidio') && columns[6].includes('total')){
          data.cantidad =  parseInt(columns[7] )
          add = true
        } else if (columns[6] === 'Número de personas desplazadas') {
          data.desplazamientos =  parseInt(columns[7])
          add = true
        }

        if(data.cantidad > 0 || data.desplazamientos > 0){
          data.id = count_homicidios
          homicidios.push(data)
        }
        
      }
      count_homicidios++
    })
    
    fs.writeFileSync('dimension_homicidios.json',JSON.stringify(homicidios,null,2),'utf-8');

    fs.writeFileSync('dimension_municipios.json',JSON.stringify(municipios,null,2),'utf-8');
    fs.writeFileSync('dimension_fechas.json',JSON.stringify(fechas,null,2),'utf-8');
    fs.writeFileSync('dimension_recursos.json',JSON.stringify(recursos,null,2),'utf-8');

    fs.writeFileSync('db.json',JSON.stringify({municipios,fechas,recursos, actividades_mineras, homicidios}),'utf-8');
    
    
  })
  
  
}


procesar()