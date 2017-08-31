def q1IsEven (number: Int): Boolean = {
    return number % 2 == 0
}
println(q1IsEven(3)) // false
println(q1IsEven(4)) // true

def q1IsEvenAlt (number: Int) = number % 2 == 0
println(q1IsEvenAlt(3)) // false
println(q1IsEvenAlt(4)) // true

def q2ContainsEven (numbers: List[Int]): Boolean = {
    for (num <- numbers) {
        if (num % 2 == 0) {
            return true
        }
    }
    return false
}
println(q2ContainsEven(List(1,3,5,7))) // false
println(q2ContainsEven(List(1,3,4,5,7))) // true

def q3LuckySevens (numbers: List[Int]): Int = {
    var summation = 0
    for (num <- numbers) {
        summation += num
        if (num == 7) {
            summation += num
        }
    }

    return summation
}
println(q3LuckySevens(List(1,3,7))) // 18
println(q3LuckySevens(List(1,3,6))) // 10

def q4FindBalance (numbers: List[Int]): Boolean = {
    for (i <- 0 to numbers.size) {
        if (numbers.drop(i).sum == numbers.take(i).sum) {
            return true
        }
    }
    return false
}
println(q4FindBalance(List(1,4,6))) // false
println(q4FindBalance(List(2,4,6))) // true

def q5PalindromeCheck (input: String): Boolean = {
    return input == input.reverse
}
println(q5PalindromeCheck("pickle")) // false
println(q5PalindromeCheck("racefastsafecar")) // true
