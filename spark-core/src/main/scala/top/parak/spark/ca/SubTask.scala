package top.parak.spark.ca

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
class SubTask extends Serializable {

  var datas : List[Int] = _

  val logic: (Int) => Int = _ * 3

  def compute() = {
    datas.map(logic)
  }

}
