import React from 'react';
import {
  AbsoluteFill,
  Easing,
  Img,
  interpolate,
  staticFile,
  useCurrentFrame,
  useVideoConfig,
} from 'remotion';

const BG = '#f8f8f6';
const INK = '#111111';
const RED = '#e11919';

const clamp = {
  extrapolateLeft: 'clamp' as const,
  extrapolateRight: 'clamp' as const,
};

export const VpnKbrLoader: React.FC = () => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();

  const intro = interpolate(frame, [0, 0.8 * fps], [0, 1], {
    ...clamp,
    easing: Easing.bezier(0.16, 1, 0.3, 1),
  });
  const progress = interpolate(frame, [0.25 * fps, 4.6 * fps], [0, 100], clamp);
  const pulse = interpolate(frame % fps, [0, fps / 2, fps], [0.22, 0.74, 0.22], clamp);
  const captionOpacity = interpolate(frame, [0.45 * fps, 1.15 * fps], [0, 1], clamp);
  const trace = interpolate(frame, [0.2 * fps, 4.6 * fps], [0, 1], clamp);
  const traceDash = 1320;

  return (
    <AbsoluteFill
      style={{
        background: BG,
        alignItems: 'center',
        justifyContent: 'center',
        color: INK,
        fontFamily:
          'Inter, Arial, Helvetica, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif',
      }}
    >
      <div
        style={{
          position: 'absolute',
          inset: 44,
          border: `1px solid ${INK}`,
          opacity: 0.08,
        }}
      />

      <div
        style={{
          position: 'relative',
          width: 620,
          height: 620,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          opacity: intro,
          transform: `translateY(${interpolate(intro, [0, 1], [20, 0])}px) scale(${interpolate(
            intro,
            [0, 1],
            [0.96, 1]
          )})`,
        }}
      >
        <div
          style={{
            position: 'absolute',
            width: 500,
            height: 500,
            borderRadius: 34,
            background: '#ffffff',
            boxShadow: `0 28px 80px rgba(0, 0, 0, ${0.07 + pulse * 0.03})`,
            overflow: 'hidden',
          }}
        >
          <Img
            src={staticFile('vpn-kbr-logo.jpg')}
            style={{
              width: '100%',
              height: '100%',
              objectFit: 'contain',
              transform: `scale(${interpolate(frame, [0, 5 * fps], [0.985, 1.015])})`,
              filter: 'grayscale(1) contrast(1.24) brightness(0.98)',
              opacity: 0.98,
            }}
          />
          <svg
            viewBox="0 0 500 500"
            style={{
              position: 'absolute',
              inset: 0,
              zIndex: 3,
              width: '100%',
              height: '100%',
              mixBlendMode: 'normal',
              opacity: 0.96,
            }}
          >
            <defs>
              <filter id="neonGlow" x="-25%" y="-25%" width="150%" height="150%">
                <feGaussianBlur stdDeviation="2.2" result="blur" />
                <feMerge>
                  <feMergeNode in="blur" />
                  <feMergeNode in="SourceGraphic" />
                </feMerge>
              </filter>
            </defs>
            <path
              d="M123 183 C155 167 190 143 224 116 C239 132 257 153 276 174 C288 153 302 137 318 122 C338 144 364 165 397 183"
              fill="none"
              stroke={RED}
              strokeWidth="4"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeDasharray={traceDash}
              strokeDashoffset={traceDash * (1 - trace)}
              filter="url(#neonGlow)"
            />
            <path
              d="M224 116 C220 136 203 158 184 174 M318 122 C323 145 344 163 372 176"
              fill="none"
              stroke={RED}
              strokeWidth="2"
              strokeLinecap="round"
              strokeDasharray={traceDash}
              strokeDashoffset={traceDash * (1 - trace)}
              opacity={0.75}
            />
            <path
              d="M248 224 C226 224 201 229 181 240 C169 247 168 258 181 266 C198 277 224 283 224 303 C224 328 190 340 154 354"
              fill="none"
              stroke={RED}
              strokeWidth="3.5"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeDasharray="420"
              strokeDashoffset={420 * (1 - Math.max(0, trace - 0.55) / 0.45)}
              filter="url(#neonGlow)"
              opacity={0.82}
            />
          </svg>
        </div>
      </div>

      <div
        style={{
          position: 'absolute',
          bottom: 122,
          width: 520,
          opacity: captionOpacity,
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'baseline',
            color: INK,
          }}
        >
          <div
            style={{
              fontSize: 22,
              lineHeight: 1,
              fontWeight: 700,
              color: INK,
              opacity: 0.72,
              letterSpacing: 0,
            }}
          >
            {Math.round(progress).toString().padStart(3, '0')}%
          </div>
        </div>
        <div
          style={{
            marginTop: 22,
            height: 4,
            background: 'rgba(7, 52, 95, 0.12)',
            overflow: 'hidden',
          }}
        >
          <div
            style={{
              width: `${progress}%`,
              height: '100%',
              background: INK,
            }}
          />
        </div>
      </div>
    </AbsoluteFill>
  );
};
