

var PREVIEW_SIZE = 1000;
var CAT_LIMIT = 64;

function ingest(file, download, callback, progress) {
    var header = true;
    var colCount;
    var fields = [];
    var arrays = [];
    var rowCount = 0;

    function ingestChunk(results) {
        var rows = results.data;

        for (i in rows) {
            if (header) {
                /* skip the first row if there's a header */
                header = false;
                continue;
            }
            var row = rows[i];
            if (row.length != colCount) {
                alert("Invalid data: variable number of columns");
                return;
            }
            for (j in row) {
                var val = row[j];
                var field = fields[j];
                if (val === "") {
                    arrays[j].push(null);
                } else if (field.optype == "numeric") {
                    if (Number.isFinite(val)) {
                        field.minimum = Math.min(field.minimum, val);
                        field.maximum = Math.max(field.maximum, val);
                        arrays[j].push(val);
                    } else {
                        arrays[j].push(null);
                    }
                } else if (field.optype == "categorical") {
                    var catIndex = field.categories[val];
                    catIndex = catIndex === undefined ? Object.keys(field.categories).length : catIndex;
                    if (catIndex >= CAT_LIMIT) {
                        field.optype == "text";
                        /* delete field.categories; */
                    } else {
                        field.categories[val] = catIndex;
                        arrays[j].push(catIndex);
                    }
                } else if (field.optype == "text") {
                    arrays[j].push(val);
                }
            }
        }
        rowCount += rows.length;
        progress(rowCount);
    }

    function finalize() {
        var finalFields = [];
        var finalArrays = [];
        for (i in fields) {
            var field = fields[i];
            if (field.optype == "categorical") {
                var catArr = [];
                for (cat in field.categories) {
                    catArr[field.categories[cat]] = cat;
                }
                field.categories = catArr;
            }
            finalFields.push(field);
            finalArrays.push(arrays[i]);
        }
        callback(finalFields, finalArrays);
    }

    function preview(results) {
        var rows = results.data;
        colCount = rows[0].length;
        var nilCounts = [];
        var numCounts = [];
        var nums = [];
        var catCounts = [];
        var cats = [];
        for (i in rows[0]) {
            nilCounts[i] = 0;
            numCounts[i] = 0;
            nums[i] = null;
            catCounts[i] = 0;
            cats[i] = {};
        }
        for (i = 1; i < rows.length; i++) {
            var row = rows[i];
            if (row.length != colCount) {
                alert("Invalid data: variable number of columns");
                return;
            }
            for (j in row) {
                var val = row[j];

                if (val == "") {
                    nilCounts[j]++;
                } else if (Number.isFinite(val)) {
                    numCounts[j]++;
                    nums[j] = val;
                } else {
                    catCounts[j]++;
                    cats[j][val] = true;
                }
            }
        }

        var firstRow = rows[0];
        for (i = 0; i < colCount; i++) {
            var headerVal = rows[0][i];
            /* header = header && headerVal != ""; */
            if (numCounts[i] > catCounts[i]) {
                header = header && !Number.isFinite(headerVal);
                fields[i] = {
                    optype: "numeric",
                    minimum: nums[i],
                    maximum: nums[i]
                };
            } else if (Object.keys(cats[i]).length < CAT_LIMIT && Object.keys(cats[i]).length > 0) {
                header = header && cats[i][headerVal] != true;
                fields[i] = {
                    optype: "categorical",
                    categories: {}
                };
            } else {
                header = header && cats[i][headerVal] != true;
                fields[i] = { optype: "text" };
            }
        }
        for (i = 0; i < colCount; i++) {
            var id = "field" + i;
            fields[i].id = id;
            fields[i].name = header ? firstRow[i] : id;
            arrays[i] = [];
        }

        Papa.parse(file, {
            worker: true,
            delimiter: ",",
            download: download,
            dynamicTyping: true,
            skipEmptyLines: true,
	    chunk: ingestChunk,
            complete: finalize
        });
    }

    Papa.parse(file, {
        worker: true,
        delimiter: ",",
        download: download,
        preview: PREVIEW_SIZE,
        dynamicTyping: true,
        skipEmptyLines: true,
        complete: preview
    });
}
