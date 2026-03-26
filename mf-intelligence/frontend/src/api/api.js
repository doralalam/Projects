const BASE_URL = "http://127.0.0.1:8000";

export const fetchMFTable = (type, limit = 10) =>
  fetch(`${BASE_URL}/mf/${type}?limit=${limit}`).then(res => res.json());

export const fetchFundsList = () =>
  fetch(`${BASE_URL}/funds-list`).then(res => res.json());

export const fetchMomPivot = (amc, fund) =>
  fetch(`${BASE_URL}/fund/${encodeURIComponent(amc)}/${encodeURIComponent(fund)}/mom-pivot`)
    .then(res => res.json());

export const fetchDrilldown = (isin) =>
  fetch(`${BASE_URL}/mf/drilldown/${encodeURIComponent(isin)}`)
    .then(res => res.json());

export const fetchAllBuckets = async (limit = 10) => {
  const buckets = ["mf_increased", "mf_decreased", "mf_fresh", "mf_exit"];
  const results = {};

  for (let bucket of buckets) {
    results[bucket] = await fetchMFTable(bucket, limit);
  }

  return results;
};