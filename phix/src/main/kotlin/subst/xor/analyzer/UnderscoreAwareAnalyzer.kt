package subst.xor.analyzer

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter

class UnderscoreAwareAnalyzer : Analyzer() {

    override fun createComponents(fieldName: String): TokenStreamComponents {
        val tokenizer = StandardTokenizer()
        var stream: TokenStream = LowerCaseFilter(tokenizer)

        // WordDelimiter 설정
        val flags =
            WordDelimiterGraphFilter.GENERATE_WORD_PARTS or
                    WordDelimiterGraphFilter.GENERATE_NUMBER_PARTS or
                    WordDelimiterGraphFilter.PRESERVE_ORIGINAL
        stream = WordDelimiterGraphFilter(
            stream,
            flags,
            null
        )

        return TokenStreamComponents(tokenizer, stream)
    }
}