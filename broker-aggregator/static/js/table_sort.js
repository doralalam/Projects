(function() {
    const table = document.getElementById('calls-table');
    if (!table) return;

    const tbody = table.querySelector('tbody');
    const headers = table.querySelectorAll('th[data-col]');
    const sortState = {};

    const getCellValue = (row, colAttr) =>
        row.querySelector('[data-' + colAttr + ']')
           .getAttribute('data-' + colAttr)
           .toUpperCase();

    const compare = (a, b, colAttr, asc) => {
        let va = getCellValue(a, colAttr);
        let vb = getCellValue(b, colAttr);
        if (va < vb) return asc ? -1 : 1;
        if (va > vb) return asc ? 1 : -1;
        return 0;
    };

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
