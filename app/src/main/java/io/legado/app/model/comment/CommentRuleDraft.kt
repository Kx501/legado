package io.legado.app.model.comment

import org.jsoup.Jsoup
import org.jsoup.nodes.Element

/**
 * 评论区规则草稿（未接入业务流程）。
 *
 * 目的：
 * - 先把规则结构和解析能力落在项目里，后续再按开关/入口接入详情页。
 * - 当前仅提供 HTML 解析工具，不依赖现有 BookInfo 调用链。
 */
data class CommentRuleDraft(
    val list: String,
    val user: String,
    val content: String,
    val time: String? = null,
    val avatar: String? = null,
    val likeCount: String? = null
)

data class CommentItemDraft(
    val user: String,
    val content: String,
    val time: String? = null,
    val avatar: String? = null,
    val likeCount: String? = null
)

object CommentParserDraft {

    /**
     * 使用 CSS 选择器从详情页 HTML 提取评论列表。
     *
     * @param html 原始 HTML 文本
     * @param rule 评论规则
     * @param baseUrl 可选 baseUrl，用于把头像等相对地址转为绝对地址
     */
    fun parse(
        html: String,
        rule: CommentRuleDraft,
        baseUrl: String? = null
    ): List<CommentItemDraft> {
        if (html.isBlank() || rule.list.isBlank()) return emptyList()
        val doc = if (baseUrl.isNullOrBlank()) Jsoup.parse(html) else Jsoup.parse(html, baseUrl)
        val items = doc.select(rule.list)
        if (items.isEmpty()) return emptyList()
        return items.mapNotNull { item ->
            val user = item.selectFirstText(rule.user)
            val content = item.selectFirstText(rule.content)
            if (user.isBlank() || content.isBlank()) return@mapNotNull null
            CommentItemDraft(
                user = user,
                content = content,
                time = rule.time?.let { item.selectFirstText(it).ifBlank { null } },
                avatar = rule.avatar?.let { item.selectFirstAttrOrText(it).ifBlank { null } },
                likeCount = rule.likeCount?.let { item.selectFirstText(it).ifBlank { null } }
            )
        }
    }

    /**
     * 内置示例：适配你给的 test.html 第一段结构。
     */
    val SAMPLE_RULE_A = CommentRuleDraft(
        list = ".comment-list .comment-item",
        user = ".comment-username a span, .comment-username a",
        content = ".comment-content",
        time = ".comment-time",
        avatar = "img.comment-avatar@src",
        likeCount = ".comment-action.like"
    )

    /**
     * 内置示例：适配你给的 test.html 第二段结构。
     */
    val SAMPLE_RULE_B = CommentRuleDraft(
        list = ".newcomment_data .newpl_ans",
        user = ".pl_info_lef",
        content = ".npl_con p",
        time = ".fun_time",
        avatar = ".npl_anlef img@src",
        likeCount = ".sel_ding"
    )
}

private fun Element.selectFirstText(selector: String): String {
    return select(selector).firstOrNull()?.text()?.trim().orEmpty()
}

/**
 * 支持 selector@attr 语法：
 * - ".comment-avatar@src" -> 取 src（会优先用 absUrl）
 * - ".comment-avatar" -> 退化为 text
 */
private fun Element.selectFirstAttrOrText(selectorOrAttr: String): String {
    val at = selectorOrAttr.lastIndexOf('@')
    if (at <= 0 || at >= selectorOrAttr.length - 1) {
        return selectFirstText(selectorOrAttr)
    }
    val selector = selectorOrAttr.substring(0, at).trim()
    val attr = selectorOrAttr.substring(at + 1).trim()
    if (selector.isBlank() || attr.isBlank()) return ""
    val target = select(selector).firstOrNull() ?: return ""
    // 有 baseUri 时优先绝对地址，避免相对路径额外处理
    val abs = target.absUrl(attr).trim()
    if (abs.isNotEmpty()) return abs
    return target.attr(attr).trim()
}

