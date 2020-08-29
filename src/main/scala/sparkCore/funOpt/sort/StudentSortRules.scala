package sparkCore.funOpt.sort

/**
  * 可以在多个隐式转换函数中定义多个排序规则
  * 在排序时只要import相应的排序规则即可
  *
  * @author jacky-wangjj
  * @date 2020/8/29
  */
object StudentSortRules {

  // 先按照分数降序，再按年龄升序进行排序
  implicit object OrderingStudentScore extends Ordering[Student4] {
    override def compare(x: Student4, y: Student4): Int = {
      if (x.score == y.score) {
        x.age - y.age
      } else if (x.score < y.score) {
        1
      } else {
        -1
      }
    }
  }

  // 先按照年龄升序，再按分数降序进行排序
  implicit object OrderingStudentAge extends Ordering[Student4] {
    override def compare(x: Student4, y: Student4): Int = {
      if (x.age == y.age) {
        if (x.score < y.score) {
          1
        } else {
          -1
        }
      } else {
        x.age - y.age
      }
    }
  }

}
