package week2

/**
  * Created by matijav on 13/03/2017.
  */
case class WikipediaArticle(title: String, text: String) {

}

object WikipediaArticle {
    def parse(line: String): WikipediaArticle = {
        val subs = "</title><text>"
        val i = line.indexOf(subs)
        val title = line.substring(14, i)
        val text  = line.substring(i + subs.length, line.length-16)
        WikipediaArticle(title, text)
    }
}
