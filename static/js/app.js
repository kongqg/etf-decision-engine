function renderLineCharts() {
    document.querySelectorAll("[data-line-chart]").forEach((node) => {
        const raw = node.getAttribute("data-line-chart");
        if (!raw) {
            return;
        }
        const data = JSON.parse(raw);
        if (!Array.isArray(data) || data.length === 0) {
            node.innerHTML = "<p class='muted'>暂无曲线数据。</p>";
            return;
        }

        const width = 640;
        const height = 220;
        const padding = 20;
        const values = data.map((item) => Number(item.total_asset));
        const max = Math.max(...values);
        const min = Math.min(...values);
        const range = Math.max(max - min, 1);

        const points = values.map((value, index) => {
            const x = padding + (index * (width - padding * 2)) / Math.max(values.length - 1, 1);
            const y = height - padding - ((value - min) / range) * (height - padding * 2);
            return `${x},${y}`;
        }).join(" ");

        const last = data[data.length - 1];
        node.innerHTML = `
            <svg class="line-chart" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
                <polyline fill="none" stroke="#0f8c6f" stroke-width="4" points="${points}"></polyline>
            </svg>
            <div class="muted">最新累计收益率：${Number(last.cumulative_return_pct).toFixed(2)}%</div>
        `;
    });
}

function renderDonuts() {
    document.querySelectorAll(".donut-chart").forEach((node) => {
        const cash = Number(node.dataset.cash || 0);
        const market = Number(node.dataset.market || 0);
        const total = Math.max(cash + market, 1);
        const marketDeg = (market / total) * 360;
        node.style.background = `conic-gradient(#145a4a 0deg ${marketDeg}deg, #f0d5c2 ${marketDeg}deg 360deg)`;
    });
}

document.addEventListener("DOMContentLoaded", () => {
    renderLineCharts();
    renderDonuts();
});
