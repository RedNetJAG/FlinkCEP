import play.api.libs.json.Json

object jsontest {
  def main(args: Array[String]) {
    
    case class MyJson(Received: String,
                      Created: String,
                      Location: Option[Location],
                      Infos: Option[List[String]],
                      DigitalInputs: Option[List[DigitalInputs]])

    case class Location(Created: String,
                        Coordinate: Coordinate)

    case class Coordinate(Latitude: Double,
                          Longitude: Double)

    case class DigitalInputs(TypeId: Option[Int],
                             Value: Option[Boolean],
                             Index: Option[Int])

    implicit val digitalInputsReads = Json.reads[DigitalInputs]
    implicit val coordinateReads = Json.reads[Coordinate]
    implicit val locationReads = Json.reads[Location]
    implicit val myJsonReads = Json.reads[MyJson]

    val inputJson = Json.parse(
      """
        |{
        |  "Received":"2015-12-29T00:00:00.000Z",
        |  "Created":"2015-12-29T00:00:00.000Z",
        |  "Location":{
        |    "Created":"2015-12-29T00:00:00.000Z",
        |    "Coordinate":{
        |      "Latitude":45.607807,
        |      "Longitude":-5.712018
        |    }
        |  },
        |  "Infos":[
        |
        |  ],
        |  "DigitalInputs":[
        |    {
        |      "TypeId":145,
        |      "Value":false,
        |      "Index":23
        |    }
        |  ]
        |}
      """.stripMargin
    )

    val myJsonInstance: MyJson = inputJson.as[MyJson]
    val longitude: Option[Double] = myJsonInstance.Location.map(_.Coordinate.Longitude) // Some(-5.712018)
    val typeId: Option[Int] = myJsonInstance.DigitalInputs.flatMap(_.headOption.flatMap(_.TypeId)) // Some(145)
}
}


Error:(22, 49) No unapply or unapplySeq function found
    implicit val digitalInputsReads = Json.reads[DigitalInputs]
                                                ^
Error:(23, 46) No unapply or unapplySeq function found
    implicit val coordinateReads = Json.reads[Coordinate]
                                             ^
Error:(24, 44) No unapply or unapplySeq function found
    implicit val locationReads = Json.reads[Location]
                                           ^
Error:(25, 42) No unapply or unapplySeq function found
    implicit val myJsonReads = Json.reads[MyJson]
                                         ^
Error:(53, 46) No Json deserializer found for type MyJson. Try to implement an implicit Reads or Format for this type.
    val myJsonInstance: MyJson = inputJson.as[MyJson]
                                             ^
Error:(53, 46) not enough arguments for method as: (implicit fjs: play.api.libs.json.Reads[MyJson])MyJson.
Unspecified value parameter fjs.
    val myJsonInstance: MyJson = inputJson.as[MyJson]
                                             ^
