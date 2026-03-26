const BASE_URL = "http://127.0.0.1:8000";

export const fetchMFTable = (type, limit = 10) =>
  fetch(`${BASE_URL}/mf/${type}?limit=${limit}`).then(res => res.json());

export const fetchFundsList = () =>
  fetch(`${BASE_URL}/funds-list`).then(res => res.json());

export const fetchMomPivot = (amc, fund) =>
  fetch(`${BASE_URL}/fund/${encodeURIComponent(amc)}/${encodeURIComponent(fund)}/mom-pivot`)
    .then(res => res.json());