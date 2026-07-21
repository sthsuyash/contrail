import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Contrail",
  description: "Flight telemetry reconstructed from OpenSky Network state vectors.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
