package it.sorintlab.solr;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.apache.solr.util.DateMathParser;

public class FileIteratorEntityProcessor extends EntityProcessorBase {

    /**
     * A regex pattern to identify files given in data-config.xml after resolving any variables
     */
    private String fileName;

    /**
     * The baseDir given in data-config.xml after resolving any variables
     */
    private String baseDir;

    /**
     * A Regex pattern of excluded file names as given in data-config.xml after resolving any variables
     */
    private String excludes;

    /**
     * The newerThan given in data-config as a {@link java.util.Date}
     * <p>
     * <b>Note: </b> This variable is resolved just-in-time in the {@link #nextRow()} method.
     * </p>
     */
    private Instant newerThan;

    /**
     * The newerThan given in data-config as a {@link java.util.Date}
     */
    private Instant olderThan;

    /**
     * The biggerThan given in data-config as a long value
     * <p>
     * <b>Note: </b> This variable is resolved just-in-time in the {@link #nextRow()} method.
     * </p>
     */
    private long biggerThan = -1;

    /**
     * The smallerThan given in data-config as a long value
     * <p>
     * <b>Note: </b> This variable is resolved just-in-time in the {@link #nextRow()} method.
     * </p>
     */
    private long smallerThan = -1;

    /**
     * The recursive given in data-config. Default value is false.
     */
    private boolean recursive = false;

    private Pattern fileNamePattern, excludesPattern;

    @Override
    public void init(Context context) {
        super.init(context);

        fileName = context.getEntityAttribute(FILE_NAME);
        if (fileName != null) {
            fileName = context.replaceTokens(fileName);
            fileNamePattern = Pattern.compile(fileName);
        }

        baseDir = context.getEntityAttribute(BASE_DIR);
        if (baseDir == null) {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                    "'baseDir' is a required attribute");
        }

        baseDir = context.replaceTokens(baseDir);
        final Path dir = Paths.get(baseDir);
        if (!Files.isDirectory(dir)) {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                    "'baseDir' value: " + baseDir + " is not a directory");
        }

        final String r = context.getEntityAttribute(RECURSIVE);
        if (r != null) {
            recursive = Boolean.parseBoolean(r);
        }

        excludes = context.getEntityAttribute(EXCLUDES);
        if (excludes != null) {
            excludes = context.replaceTokens(excludes);
            excludesPattern = Pattern.compile(excludes);
        }
    }

    /**
     * Get the Date object corresponding to the given string.
     *
     * @param dateStr the date string. It can be a DateMath string or it may have a evaluator function
     * @return a Date instance corresponding to the input string
     */
    private Instant getDate(String dateStr) {
        if (dateStr == null) {
            return null;
        }

        try {
            return resolvePlaceholder(dateStr, Date.class, Date::toInstant);
        } catch (WasAStringException e) {
            dateStr = e.toString();
        }

        final Matcher m = IN_SINGLE_QUOTES_PATTERN.matcher(dateStr);
        if (m.find()) {
            String expr = m.group(1);
            if (expr.startsWith("NOW")) {
                expr = expr.substring("NOW".length());
            }
            try {
                return new DateMathParser(TimeZone.getDefault()).parseMath(expr).toInstant();
            } catch (ParseException e) {
                throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "Invalid expression for date",
                        e);
            }
        }

        try {
            return LocalDateTime.parse(dateStr, DATE_FORMAT).atZone(ZoneId.systemDefault()).toInstant();
        } catch (DateTimeParseException e) {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "Invalid expression for date", e);
        }
    }

    /**
     * Get the Long value for the given string after resolving any evaluator or variable.
     *
     * @param sizeStr the size as a string
     * @return the Long value corresponding to the given string
     */
    private long getSize(String sizeStr) {
        if (sizeStr == null) {
            return -1;
        }

        try {
            return resolvePlaceholder(sizeStr, Number.class, Number::longValue);
        } catch (WasAStringException e) {
            return Long.parseLong(e.toString());
        }
    }

    private static class WasAStringException extends Exception {
        private static final long serialVersionUID = 1L;

        public WasAStringException(final String value) {
            super(value);
        }

        @Override
        public String toString() {
            return super.getMessage();
        }
    }

    private <T, R> R resolvePlaceholder(final String string, final Class<T> type, final Function<T, R> tx)
            throws WasAStringException {
        final Matcher m = PLACEHOLDER_PATTERN.matcher(string);
        if (m.find()) {
            final Object o = context.resolve(m.group(1));
            if (type.isInstance(o)) {
                return tx.apply(type.cast(o));
            }
            throw new WasAStringException((String) o);
        } else {
            throw new WasAStringException(context.replaceTokens(string));
        }
    }

    @Override
    public Map<String, Object> nextRow() {
        if (rowIterator != null) {
            return getNext();
        }

        final Path dir = Paths.get(baseDir);

        final String newerThanStr = context.getEntityAttribute(NEWER_THAN);
        newerThan = getDate(newerThanStr);
        final String olderThanStr = context.getEntityAttribute(OLDER_THAN);
        olderThan = getDate(olderThanStr);

        final String biggerThanStr = context.getEntityAttribute(BIGGER_THAN);
        biggerThan = getSize(biggerThanStr);
        final String smallerThanStr = context.getEntityAttribute(SMALLER_THAN);
        smallerThan = getSize(smallerThanStr);

        rowIterator = getFilesIterator(dir);

        return getNext();
    }

    private Stream<Path> getFolderFiles(final Path dir) {
        try {
            return Files.list(dir).flatMap(path -> {
                if (Files.isDirectory(path)) {
                    if (recursive)
                        return getFolderFiles(path);
                    return Stream.empty();
                }
                return Stream.of(path);
            }).filter(this::matchesFilename);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean matchesFilename(final Path path) {
        final String name = path.getFileName().toString();
        return fileNamePattern == null || fileNamePattern.matcher(name).find()
                && (excludesPattern == null || excludesPattern.matcher(name).find());
    }

    private Iterator<Map<String, Object>> getFilesIterator(final Path dir) {
        return getFolderFiles(dir).flatMap(path -> {
            try {
                final long size = Files.size(path);
                if (!matchesSize(size))
                    return Stream.empty();

                final Instant lastModified = Files.getLastModifiedTime(path).toInstant();
                if (!matchesDate(lastModified))
                    return Stream.empty();

                final Map<String, Object> details = new HashMap<>();

                details.put(DIR, path.getParent().toAbsolutePath().toString());
                details.put(FILE, path.getFileName().toString());
                details.put(ABSOLUTE_FILE, path.toAbsolutePath().toString());
                details.put(SIZE, size);
                details.put(LAST_MODIFIED, Date.from(lastModified));

                return Stream.of(details);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }).iterator();
    }

    private boolean matchesSize(final long size) {
        return (biggerThan == -1 || size > biggerThan) && (smallerThan == -1 || size < smallerThan);
    }

    private boolean matchesDate(final Instant lastModified) {
        return (olderThan == null || lastModified.isBefore(olderThan))
                && (newerThan == null || lastModified.isAfter(newerThan));
    }

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");

    private static final Pattern IN_SINGLE_QUOTES_PATTERN = Pattern.compile("^'(.*?)'$");

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String DIR = "fileDir";

    private static final String FILE = "file";

    private static final String ABSOLUTE_FILE = "fileAbsolutePath";

    private static final String SIZE = "fileSize";

    private static final String LAST_MODIFIED = "fileLastModified";

    private static final String FILE_NAME = "fileName";

    private static final String BASE_DIR = "baseDir";

    private static final String EXCLUDES = "excludes";

    private static final String NEWER_THAN = "newerThan";

    private static final String OLDER_THAN = "olderThan";

    private static final String BIGGER_THAN = "biggerThan";

    private static final String SMALLER_THAN = "smallerThan";

    private static final String RECURSIVE = "recursive";

}
