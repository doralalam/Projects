(function() {

    const table = document.getElementById('calls-table');
    if (!table) return;

    const tbody = table.querySelector('tbody');
    const headers = table.querySelectorAll('th[data-col]');
    const sortState = {};

    // -----------------------------
    // GET CELL VALUE
    // -----------------------------
    const getCellValue = (row, colAttr) => {

        const el = row.querySelector('[data-' + colAttr + ']');
        if (!el) return "";

        return el.getAttribute('data-' + colAttr);
    };

    // -----------------------------
    // DETECT TYPE
    // -----------------------------
    const parseValue = (val) => {

        if (val === null || val === "") return null;

        // Try number
        const num = parseFloat(val);
        if (!isNaN(num)) return num;

        // Try date
        const date = Date.parse(val);
        if (!isNaN(date)) return date;

        // Fallback text
        return val.toUpperCase();
    };

    // -----------------------------
    // COMPARATOR
    // -----------------------------
    const compare = (a, b, colAttr, asc) => {

        let va = parseValue(getCellValue(a, colAttr));
        let vb = parseValue(getCellValue(b, colAttr));

        // Handle nulls → push to bottom
        if (va === null) return 1;
        if (vb === null) return -1;

        if (va < vb) return asc ? -1 : 1;
        if (va > vb) return asc ? 1 : -1;
        return 0;
    };

    // -----------------------------
    // HEADER CLICK
    // -----------------------------
    headers.forEach(th => {

        th.addEventListener('click', () => {

            const col = th.getAttribute('data-col');
            const asc = !sortState[col];
            sortState[col] = asc;

            const rows = Array.from(tbody.querySelectorAll('tr'));

            rows.sort((a, b) => compare(a, b, col, asc));

            rows.forEach(r => tbody.appendChild(r));
        });
    });

})();
