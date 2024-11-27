// 1. Consultas básicas: inserción, selección, actualización y eliminación de documentos

// Obtener un registro
db['songs-2023'].find({"track_name": "City of Gods"})

// Insertar un registro
db['songs-2023'].insertOne({
  "track_name": "Nueva canción",
  "artist(s)_name": "Nuevo artista",
  "artist_count": 1,
  "released_year": 2023,
  "streams": 1000000,
  "bpm": 120
})

// Actualizar un registro
db['songs-2023'].updateOne(
  { "track_name": "Columbia" },
  { $set: { "streams": 150000000 } }
)

// Eliminar de un registro
db['songs-2023'].deleteOne({ "track_name": "Nueva canción" })


// 2. Consultas con filtros y operadores

// Filtrar canciones lanzadas en 2023 con más de 100 millones de streams
db['songs-2023'].find(
  { "released_year": 2023, "streams": { $gt: 100000000 } }
)

// Filtrar canciones que tienen un BPM entre 120 y 130
db['songs-2023'].find(
  { "bpm": { $gte: 120, $lte: 130 } }
)

// Filtrar canciones con más de un artista (artist_count mayor a 1)
db['songs-2023'].find(
  { "artist_count": { $gt: 1 } }
)


// 3. Consultas de agregación para calcular estadísticas

// Contar la cantidad total de documentos en la colección
db['songs-2023'].countDocuments()

// Calcular el promedio de BPM de todas las canciones
db['songs-2023'].aggregate([
  { $group: { _id: null, bpm_promedio: { $avg: "$bpm" } } }
])

// Obtener la cantidad de canciones por año de lanzamiento
db['songs-2023'].aggregate([
  { $group: { _id: "$released_year", cantidad: { $sum: 1 } } },
  { $sort: { _id: -1 } }
])
