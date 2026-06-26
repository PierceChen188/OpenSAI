// Inline SVG icons (stroke-based, currentColor). Bundled — no icon font / CDN,
// keeping the build fully offline (spec §15).

const S = ({ children, size = 18, ...p }) => (
  <svg
    width={size}
    height={size}
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.7"
    strokeLinecap="round"
    strokeLinejoin="round"
    {...p}
  >
    {children}
  </svg>
);

export const Icon = {
  cube: (p) => (
    <S {...p}>
      <path d="M12 2 3 7v10l9 5 9-5V7z" />
      <path d="M3 7l9 5 9-5M12 12v10" />
    </S>
  ),
  grid: (p) => (
    <S {...p}>
      <rect x="3" y="3" width="7" height="7" rx="1" />
      <rect x="14" y="3" width="7" height="7" rx="1" />
      <rect x="3" y="14" width="7" height="7" rx="1" />
      <rect x="14" y="14" width="7" height="7" rx="1" />
    </S>
  ),
  flow: (p) => (
    <S {...p}>
      <circle cx="5" cy="6" r="2.5" />
      <circle cx="19" cy="6" r="2.5" />
      <circle cx="12" cy="18" r="2.5" />
      <path d="M5 8.5v3a2 2 0 0 0 2 2h2M19 8.5v3a2 2 0 0 1-2 2h-2" />
    </S>
  ),
  layers: (p) => (
    <S {...p}>
      <path d="M12 2 2 7l10 5 10-5z" />
      <path d="M2 12l10 5 10-5M2 17l10 5 10-5" />
    </S>
  ),
  cpu: (p) => (
    <S {...p}>
      <rect x="7" y="7" width="10" height="10" rx="1.5" />
      <path d="M9 1v3M15 1v3M9 20v3M15 20v3M1 9h3M1 15h3M20 9h3M20 15h3" />
    </S>
  ),
  box: (p) => (
    <S {...p}>
      <path d="M21 8V7l-9-4-9 4v10l9 4 9-4v-6" />
      <path d="M3 7l9 4 9-4" />
    </S>
  ),
  sliders: (p) => (
    <S {...p}>
      <path d="M4 21v-7M4 10V3M12 21v-9M12 8V3M20 21v-5M20 12V3M1 14h6M9 8h6M17 16h6" />
    </S>
  ),
  list: (p) => (
    <S {...p}>
      <path d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01" />
    </S>
  ),
  play: (p) => (
    <S {...p}>
      <path d="M6 4l14 8-14 8z" />
    </S>
  ),
  plus: (p) => (
    <S {...p}>
      <path d="M12 5v14M5 12h14" />
    </S>
  ),
  trash: (p) => (
    <S {...p}>
      <path d="M3 6h18M8 6V4h8v2M6 6l1 14h10l1-14" />
    </S>
  ),
  upload: (p) => (
    <S {...p}>
      <path d="M12 16V4M7 9l5-5 5 5M4 20h16" />
    </S>
  ),
  refresh: (p) => (
    <S {...p}>
      <path d="M21 12a9 9 0 1 1-3-6.7L21 8M21 3v5h-5" />
    </S>
  ),
  link: (p) => (
    <S {...p}>
      <path d="M10 14a5 5 0 0 0 7 0l3-3a5 5 0 0 0-7-7l-1 1" />
      <path d="M14 10a5 5 0 0 0-7 0l-3 3a5 5 0 0 0 7 7l1-1" />
    </S>
  ),
  pin: (p) => (
    <S {...p}>
      <path d="M12 21s7-6.3 7-11a7 7 0 1 0-14 0c0 4.7 7 11 7 11z" />
      <circle cx="12" cy="10" r="2.5" />
    </S>
  ),
  close: (p) => (
    <S {...p}>
      <path d="M18 6 6 18M6 6l12 12" />
    </S>
  ),
  chevron: (p) => (
    <S {...p}>
      <path d="M9 18l6-6-6-6" />
    </S>
  ),
  check: (p) => (
    <S {...p}>
      <path d="M20 6 9 17l-5-5" />
    </S>
  ),
  zap: (p) => (
    <S {...p}>
      <path d="M13 2 3 14h7l-1 8 10-12h-7z" />
    </S>
  ),
  server: (p) => (
    <S {...p}>
      <rect x="3" y="4" width="18" height="7" rx="1.5" />
      <rect x="3" y="13" width="18" height="7" rx="1.5" />
      <path d="M7 7.5h.01M7 16.5h.01" />
    </S>
  ),
  warn: (p) => (
    <S {...p}>
      <path d="M12 3 2 20h20z" />
      <path d="M12 9v5M12 17h.01" />
    </S>
  ),
  inbox: (p) => (
    <S {...p}>
      <path d="M22 12h-6l-2 3h-4l-2-3H2" />
      <path d="M5 5h14l3 7v6a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1v-6z" />
    </S>
  ),
};
