package io.example.pageviews.model;

/**
 * A single page-view event.
 *
 * @param page      logical page name, e.g. "home"
 * @param visitor   anonymous visitor id
 * @param viewedAt  event time, epoch milliseconds
 * @param dwellMs   how long the visitor stayed on the page, in milliseconds
 */
public record PageView(String page, String visitor, long viewedAt, long dwellMs) {

    public static PageView now(String page, String visitor, long dwellMs) {
        return new PageView(page, visitor, System.currentTimeMillis(), dwellMs);
    }
}
